module BgExecutor
  class Daemon
    DEFAULT_CONCURRENCY = 4
    DEFAULT_QUEUE_TIMEOUT = 300
    DEFAULT_REGULAR_JOB_TIMEOUT = 1.hour

    def initialize
      @daemon_groups = {}
      client.load_regular_jobs
    end

    def execute_job
      job = client.pop
      return unless job

      wait_till_fork_allowed!(:jobs) do
        log "Executing job ##{job[:id]}"
        @daemon_groups[:jobs] = Daemons.run_proc('bg_executor_job.rb', self.daemon_options) do
          begin
            self.reconnect!
            ::BgExecutor::Executor.new.execute_job(job)
          rescue Exception => e
            log e.message
            log e.backtrace.join("\n")
            client.fail_job!(job[:id].to_i, e) if job
          ensure
            $running = false
            exit()
          end
        end
      end

    rescue Timeout::Error
      client.fail_job! job[:id].to_i, BgExecutor::QueueError.new('BgExecutor queue is full. Timeout error.')
      log "Timeout::Error cannot push job(#{job[:id]}) into queue"
    rescue Exception => e
      log e.message
      log e.backtrace.join("\n")
      client.fail_job!(job[:id].to_i, e) if job
    end

    def execute_regular_jobs
      return unless client.has_regular_jobs
      job = nil
      client.regular_jobs.each do |j|
        job = j
        last_run = client.get_last_run_for_regular_job(job[:name])
        if !last_run || last_run + job[:interval] <= Time.now
          client.set_last_run_for_regular_job(job[:name], Time.at((Time.now.to_i - (Time.now.to_i % job[:interval])))) # округляем

          unless Blocker.locked?("job_regular_#{job[:name]}")
            wait_till_fork_allowed!(:regular_jobs) do
              log "Executing regular job ##{job[:name]}"
              Blocker.lock("job_regular_#{job[:name]}", DEFAULT_REGULAR_JOB_TIMEOUT)
              @daemon_groups[:regular_jobs] = Daemons.run_proc('bg_executor_regular_job.rb', self.daemon_options) do
                begin
                  self.reconnect!
                  ::BgExecutor::Executor.new.execute_regular_job(job)
                rescue Exception => e
                  log e.message
                  log e.backtrace.join("\n")
                ensure
                  $running = false
                  Blocker.unlock("job_regular_#{job[:name]}")
                  exit()
                end
              end
            end
          else
            log "Skiping regular job #{job[:name]}"
          end
        end
      end
    rescue Exception => e
      log e.message
      log e.backtrace.join("\n")
    end

    def reconnect!
      ActiveRecord::Base.logger.level = Logger::Severity::UNKNOWN
      ActiveRecord::Base.logger = Logger.new('/dev/null')
      ActiveRecord::Base.connection.reconnect!
      ActiveRecord::Base.verify_active_connections!
      Blocker.reconnect!
      client.reconnect!
    end

    def get_concurrency
      @concurrency ||= BgExecutor::Configuration[:bg_executor][:concurrency] || DEFAULT_CONCURRENCY
    end

    def get_queue_timeout
      @queue_timeout ||= BgExecutor::Configuration[:bg_executor][:queue_timeout] || DEFAULT_QUEUE_TIMEOUT
    end

    def daemon_options(command = :start)
      {:multiple => true,
       :ontop => false,
       :backtrace => true,
       :dir_mode => :normal,
       :dir => RAILS_ROOT + '/log',
       :log_dir => RAILS_ROOT + '/log',
       :log_output => true,
       :monitor => false,
       :ARGV => [command.to_s]
      }
    end

    def client
      @client ||= BgExecutor::Client.new
    end

    protected

    def wait_till_fork_allowed!(group)
      if allowed_to_fork?(group)
        yield
        return
      else
        log "Queue is full. Waiting..."
      end
      
      Timeout::timeout(get_queue_timeout) do
        loop do
          if allowed_to_fork?(group)
            yield
            break
          end
          sleep 5
        end
      end
    end

    def allowed_to_fork?(group)
      executors_count(group) < get_concurrency
    end

    def executors_count(group)
      if @daemon_groups[group]
        @daemon_groups[group].find_applications(@daemon_groups[group].pidfile_dir).size
      else
        0
      end
    end

    # Log message to stdout
    def log(message)
      puts "%s: %s" % [Time.now.to_s, message]
    end
  end
end
