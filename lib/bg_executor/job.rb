module BgExecutor
  class Job
    class_inheritable_accessor :singleton_job

    class << self
      def create(id, job_name, params)
        real_job_name = "#{job_name}_job"

        require "#{RAILS_ROOT}/app/jobs/#{real_job_name}"

        class_name = real_job_name.classify

        klass = class_name.constantize
        klass.new(id, params)
      rescue LoadError, MissingSourceFile, NameError
        raise "No such job #{job_name}, #{class_name}"
      end

      # указать, что только один джоб этого класса может выполняться в одну единицу времени
      # можно также задать указать параметры джоба, и тогда только один джоб с такой комбинацией параметров может выполняться в одну единицу времени
      def acts_as_singleton(scope = [])
        self.singleton_job = Array(scope)
      end

      def acts_as_singleton?
        !self.singleton_job.nil?
      end

      def singleton_scope
        self.singleton_job
      end
    end

    def initialize(id, params)
      @id = id
      raise "No such job in queue" unless client.job_exists?(@id)

      @info = {}
      @result = nil
      @error  = nil

      @params = params

    end

    def id
      @id
    end

    def result
      @result
    end

    def result=(value)
      @result = value
      client.update_job!(id, :result, @result)
    end

    def info
      @info
    end

    def info=(value)
      @info = value
      client.update_job!(id, :info, @info)
    end

    def execute
      # override in descendants
    end

    def params
      @params
    end

    private
    def client
      @client ||= BgExecutor::Client.new
    end
  end

  # класс для джобов, которые можно проецировать в прогресс-бар
  class Job::Indicated < Job
    def initialize(id, params)
      super id, params
      self.info = {:completed => 0.0}
      @total = 1
      @completed = 0
    end

    # указать, сколько всего итемов в джобе
    def total=(total_items)
      raise ArgumentError unless total_items.is_a?(Integer)
      @total = total_items
    end

    # указать, сколько итемов в джобе завершено
    def completed=(completed_items)
      raise ArgumentError unless completed_items.is_a?(Integer)
      @completed = completed_items
      update_percentage!
    end

    def increment_completed!
      self.completed = @completed + 1
    end

    def update_percentage!
      self.info = self.info.merge(:completed => ((@completed.to_f / [@total, 1].max.to_f) * 100).round)
    end
  end

  # клас для регулярных задач
  class Job::Regular
    class << self
      def create(job_name, params)
        real_job_name = "#{job_name}_job"

        require "#{RAILS_ROOT}/app/jobs/#{real_job_name}"

        class_name = real_job_name.classify

        klass = class_name.constantize
        klass.new(params)
      rescue LoadError, MissingSourceFile, NameError
        raise "No such job #{job_name}, #{class_name}"
      end
    end

    def initialize(params)
      @params = params
    end

    def params
      @params
    end

    def execute
      # override in descendants
    end
  end
end