module BgExecutor
  class Daemon
    DEFAULT_CONCURRENCY = 4
    DEFAULT_QUEUE_TIMEOUT = 300

    def initialize
      if File.exist?(file = "#{RAILS_ROOT}/app/jobs/schedule.rb")
        require file
        @has_regular_jobs = BgExecutor::Schedule.tasks.present? && !BgExecutor::Schedule.tasks.empty?
      end
    end

    def execute_job
      job = client.pop
      return unless job

      wait_till_fork_allowed! do
        puts "Executing job ##{job[:id]}"
        @daemon_group = Daemons.run_proc('bg_executor_job.rb', daemon_options) do
          db_reconnect!

          begin
            ::BgExecutor::Executor.new.execute_job(job)
          rescue => e
            puts e.message
            puts e.backtrace.join("\n")
            client.fail_job!(job[:id].to_i, e) if job
          end
          exit()
        end
      end

    rescue Timeout::Error
      client.fail_job! job[:id].to_i, BgExecutor::QueueError.new('BgExecutor queue is full. Timeout error.')
      puts "Timeout::Error cannot push job(#{job[:id]}) into queue"
    rescue => e
      puts e.message
      puts e.backtrace.join("\n")
      client.fail_job!(job[:id].to_i, e) if job
    end

    def execute_regular_jobs
      return unless @has_regular_jobs

      BgExecutor::Schedule.tasks.each do |task|
        if !task[:last_run_at] || task[:last_run_at] + task[:interval] <= Time.now
          unless Blocker.send("job_regular_#{task[:name]}_locked?".to_sym, DEFAULT_QUEUE_TIMEOUT)

            puts "Executing regular job #{task[:name]}"

            Daemons.run_proc('bg_executor_regular_job.rb', daemon_options) do
              db_reconnect!

              begin
                Blocker.send("lock_job_regular_#{task[:name]}".to_sym, DEFAULT_QUEUE_TIMEOUT) do
                  ::BgExecutor::Executor.new.execute_regular_job(task[:name], task[:args])
                end
              rescue => e
                puts e.message
                puts e.backtrace.join("\n")
              end
              exit()
            end
          end
          task[:last_run_at] = Time.at((Time.now.to_i - (Time.now.to_i % 10))) # округляем до 10 секунд
        end
      end
    rescue => e
      puts e.message
      puts e.backtrace.join("\n")
    end

    def db_reconnect!
      ActiveRecord::Base.logger.level = Logger::Severity::UNKNOWN
      ActiveRecord::Base.logger = Logger.new('/dev/null')
      ActiveRecord::Base.connection.reconnect!
      ActiveRecord::Base.verify_active_connections!
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

    def wait_till_fork_allowed!
      if allowed_to_fork?
        yield
        return
      else
        puts "Queue is full. Waiting..."
      end
      
      Timeout::timeout(get_queue_timeout) do
        loop do
          if allowed_to_fork?
            yield
            break
          end
          sleep 5
        end
      end
    end

    def allowed_to_fork?
      executors_count < get_concurrency
    end

    def executors_count
      if @daemon_group
        @daemon_group.find_applications(@daemon_group.pidfile_dir).size
      else
        0
      end
    end
  end
end
