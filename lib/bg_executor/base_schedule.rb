module BgExecutor
  class BaseSchedule
    MIN_INTERVAL = 30
    class_inheritable_accessor :tasks


    def self.every(interval)
      @last_interval = interval
      yield
    end

    def self.add_task(name, interval, args = {})
      raise "Интервал должен быть не менее #{MIN_INTERVAL} секунд" if interval < MIN_INTERVAL
      self.tasks ||= []
      self.tasks << {:name => name.to_sym,
                     :interval => interval,
                     :args => args,
                     :last_run_at => Time.now.at_beginning_of_day + ((Time.now - Time.now.at_beginning_of_day) / interval).to_i * interval} # это нужно для того, чтобы например пятиминутные джобы запускались ровно в 5,10,15.. минут
    end

    def self.method_missing(m, *args)
      add_task m, @last_interval, args.extract_options!
    end

  end
end