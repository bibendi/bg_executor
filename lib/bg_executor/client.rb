module BgExecutor
  # Классс для клиентов BgExecutor
  # В инстансе мы можем:
  #   поставить задачу в очередь,
  #   спросить статус задачи,
  #   узнать информацию о задаче,
  #   спросить конечный результат
  class Client
    QUEUE_KEY = "bg_executor:jobs_queue"
    RUNNING_JOBS_KEY = "bg_executor:jobs_running"
    STASH_KEY = "bg_executor:jobs_stash"
    SEQUENCE_KEY = "bg_executor:jobs_sequence"
    JOBS_KEY_PREFIX = "bg_executor:job:"
    RJOBS_KEY_PREFIX = "bg_executor:rjob:"
    JOB_MAX_RUN_TIME = 3.hour.to_f

    # constructor
    def initialize
      @cache = BgExecutor::Redis.new

      @cache.delete QUEUE_KEY unless @cache.list? QUEUE_KEY
    end

    def redis
      @cache
    end

    def reconnect!
      @cache.redis.client.reconnect
    end

    # поставить задачу в очередь
    # возвращает два значения: ID задачи и ключ доступа к задаче
    def queue_job!(job_name, *args)
      args = args.extract_options!
      raise ArgumentError, 'job_name must be String or Symbol' unless job_name.is_a?(String) || job_name.is_a?(Symbol)
      raise ArgumentError, 'job arguments must be Hash' unless args.is_a?(Hash)

      # попробовать загрузить класс
      is_running = singleton_job_running? job_name, args
      return is_running if is_running

      id = next_id
      secure = generate_secure_key

      args[:created_at] = Time.now.to_i

      raise QueueError if id.nil?

      @cache.push QUEUE_KEY, :id => id,
                             :job_name => job_name,
                             :args => args

      @cache[job_key(id)] = {
              :id => id,
              :secure_key => secure,
              :job_name => job_name,
              :args => args,
              :status => :new,
              :info => {},
              :error => nil,
              :result => nil,
              :queued_at => Time.now.to_f,
              :started_at => nil,
              :finished_at => nil,
              :failed_at => nil
      }

      [id, secure]
    end
    alias_method :push_job!, :queue_job!

    def singleton_job_running?(job_name, args)
      raise ArgumentError, 'job_name must be String or Symbol' unless job_name.is_a?(String) || job_name.is_a?(Symbol)
      raise ArgumentError, 'job arguments must be Hash' unless args.is_a?(Hash)

      # попробовать загрузить класс
      require "jobs/#{job_name}_job"

      klass = "#{job_name}_job".classify.constantize
      # если это синглтон, попытаться найти такой же джоб среди выполняемых
      if klass.acts_as_singleton?
        matches = all_jobs { |_job|
          _job_args = (_job[:args] || {}).select { |k, v| klass.singleton_scope.include? k }.to_hash
          _args     = (args || {}).select { |k, v| klass.singleton_scope.include? k }.to_hash
          _job[:job_name].to_sym == job_name.to_sym && _job_args == _args
        }

        return nil if matches.empty?

        # если job выполняется слишком долго, то убить его
        t_0 = Time.now.to_f
        matches.reject! do |job|
          if job[:status] == :running && job[:started_at].present? && (t_0 - job[:started_at] > JOB_MAX_RUN_TIME)
            fail_job!(job[:id], 'Максимальное время выполнения задачи превышено')
            true
          end
        end

        unless matches.empty?
          job = matches.first
          return [job[:id], job[:secure_key]]
        end
      end

      return nil
    rescue Exception => e
      puts "Error in BgExecutor::Client#singleton_job_running?"
      puts e.message
      return nil
    end

    def job_exists?(job_id, secure_key = nil)
      exists = @cache.exists?(job_key job_id)

      raise JobAccessError if exists && secure_key.present? && !secure_key_matches?(job_id, secure_key)

      exists
    end

    # получить статус задачи
    def ask_status(job_id, secure_key = nil)
      return nil unless job_exists? job_id, secure_key

      raise JobAccessError unless secure_key_matches?(job_id, secure_key)

      (find_job(job_id) || {})[:status]
    end

    # получить информацию из задачи
    def ask_info(job_id, secure_key = nil)
      return nil unless job_exists? job_id, secure_key

      raise JobAccessError unless secure_key_matches?(job_id, secure_key)

      (find_job(job_id) || {})[:info]
    end

    # получить результат выполнения задачи
    def ask_result(job_id, secure_key = nil)
      return nil unless job_exists? job_id, secure_key

      raise JobAccessError unless secure_key_matches?(job_id, secure_key)

      j = find_job(job_id) || {}
      raise JobExecutionError, j[:error] unless j[:error].blank?

      j[:result]
    end

    # проверить ключ к задаче на зуб
    def secure_key_matches?(job_id, secure_key)
      return true if secure_key.nil?
      find_job(job_id)[:secure_key] == secure_key
    end

    # получить информацию о всех задачах
    # можно передать block, принимающий аргумент job и возвращающий true/false и тогда можно отфильтровать список
    def all_jobs
      jobs = (@cache[RUNNING_JOBS_KEY] || {}).values.sort{ |a, b| b <=> a }.collect { |q| find_job(q) } +
             (@cache.list(QUEUE_KEY) || []).map { |q| q.is_a?(Hash) ? find_job(q[:id]) : nil } +
             (@cache[STASH_KEY] || {}).values.sort{ |a,b| b <=> a }.collect { |q| find_job(q) }
      jobs.compact!
      
      return [] if jobs.blank?
      return jobs.select { |job| yield(job) } if block_given?

      jobs
    end

    # обновить информацию о задании
    def update_job!(job_id, key, value)
      @cache[job_key job_id] = @cache[job_key job_id].merge(key => value)
    rescue Exception => e
      puts "Error in BgExecutor::Client#update_job!"
      puts e.message
    end

    def start_job!(job_id)
      update_job! job_id, :status, :running
      update_job! job_id, :started_at, Time.now.to_f

      remove_from_stash job_id
      add_to_running job_id
    rescue Exception => e
      puts "Error in BgExecutor::Client#start_job!"
      puts e.message
    end

    # считать задание завершенным
    def finish_job!(job_id)
      if (job = find_job(job_id))
        update_job! job_id, :status, :finished
        update_job! job_id, :finished_at, Time.now.to_f
        update_job!(job_id, :info, job[:info].merge(:execution_time => "%.2f" % [Time.now.to_f - job[:started_at]])) if job[:started_at].present?
      end
      @cache.expire job_key(job_id), 600

      remove_from_running job_id
    rescue Exception => e
      puts "Error in BgExecutor::Client#finish_job!"
      puts e.message
    end

    # считать задание проваленным
    def fail_job!(job_id, exception)
      if exception.is_a?(::Exception)
        error = [exception.message, exception.backtrace.present? ? exception.backtrace.join("\n") : ''].join("\n")
      else
        error = exception.to_s
      end

      if (job = find_job(job_id))
        update_job! job_id, :status, :failed
        update_job! job_id, :error, error
        update_job! job_id, :failed_at, Time.now.to_f
        update_job!(job_id, :info, job[:info].merge(:execution_time => "%.2f" % [Time.now.to_f - job[:started_at]])) if job[:started_at].present?
      end
      @cache.expire job_key(job_id), 600

      remove_from_running job_id
    rescue Exception => e
      puts "Error in BgExecutor::Client#fail_job!"
      puts e.message
    end

    # получить из очереди задание (Оно оттуда сразу удаляется)
    def pop
      @cache.synchronize("bg_executor:queue_mutex") do
        job = nil
        if (queue_size = @cache.list_length(QUEUE_KEY).to_i) > 0
          queue_size.times do
            job = @cache.shift(QUEUE_KEY)

            # если задание отложенное и его время ещё не пришло, то кладём его обратно в конец списка
            if job[:args][:delay] && (Time.now.to_i - job[:args][:created_at].to_i) < job[:args][:delay]
              @cache.push(QUEUE_KEY, job)
              job = nil
            # если задание просроченное, то и выполнять его не надо
            elsif job[:args][:expire] && (Time.now.to_i - job[:args][:created_at].to_i) > job[:args][:expire]
              job = nil
            end

            break unless job.nil?
          end if queue_size > 0

          add_to_stash(job[:id]) if job.present?
        end
        job
      end
    rescue Exception => e
      puts "Error in BgExecutor::Client#pop"
      puts e.message
      puts e.backtrace.join("\n")
    end

    def clear_all!
      @cache[SEQUENCE_KEY] = 1
      @cache[RUNNING_JOBS_KEY] = {}
      @cache[QUEUE_KEY] = {}
      @cache[STASH_KEY] = {}
    end

    attr_accessor :has_regular_jobs
    attr_accessor :regular_jobs

    def load_regular_jobs
      unless File.exist?(file = "#{RAILS_ROOT}/app/jobs/schedule.rb")
        @has_regular_jobs = false
        return
      end

      require file
      @has_regular_jobs = BgExecutor::Schedule.tasks.present? && !BgExecutor::Schedule.tasks.empty?

      @regular_jobs = BgExecutor::Schedule.tasks.dup
      @regular_jobs.each do |task|
        set_last_run_for_regular_job(task[:name], task[:last_run_at])
      end
    end

    # храним время последнего запуска в редисе, чтобы можно было запускать на нескольких серверах
    def set_last_run_for_regular_job(job_name, time)
      @cache[rjob_key(job_name)] = time.to_i
    end

    def get_last_run_for_regular_job(job_name)
      Time.at(@cache[rjob_key(job_name)].to_i)
    end

    protected
    def next_id
      @cache.increment(SEQUENCE_KEY)
    end

    def find_job(id)
      raise ArgumentError, "Integer expected, #{id.class.name} given" if id.to_i <= 0
      @cache[job_key id]
    end

    def job_key(id)
      "#{JOBS_KEY_PREFIX}#{id}"
    end

    def rjob_key(name)
      "#{RJOBS_KEY_PREFIX}#{name}"
    end

    # удалить из стека текущих задач какой-нибудь джоб
    def remove_from_running(job_id)
      @cache.synchronize("bg_executor:running_mutex") do
        current = @cache[RUNNING_JOBS_KEY] || {}
        current.delete job_id
        @cache[RUNNING_JOBS_KEY] = current
      end
    end

    # Добавить в список выполняемых задач какой-нибудь джоб
    def add_to_running(job_id)
      return unless job_exists? job_id

      @cache.synchronize("bg_executor:running_mutex") do
        current = @cache[RUNNING_JOBS_KEY] || {}
        current[job_id] = job_id
        @cache[RUNNING_JOBS_KEY] = current
      end
    end

    def add_to_stash(job_id)
      current = @cache[STASH_KEY] || {}
      current[job_id] = job_id
      @cache[STASH_KEY] = current
    end

    def remove_from_stash(job_id)
      current = @cache[STASH_KEY] || {}
      current.delete(job_id)
      @cache[STASH_KEY] = current
    end

    require 'digest/sha2'
    def generate_secure_key
      (Digest::SHA2.new << rand.to_s).to_s
    end
  end
end