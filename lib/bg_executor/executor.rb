module BgExecutor
  class Executor

    # constructor
    def initialize
      #
    end

    # @return BgExecutor::Client
    def client
      @client ||= BgExecutor::Client.new
    end

    # execute job
    # will fail if Rails not loaded
    def execute_job(job_hash)
      $0 = "Job ##{job_hash[:id]}: #{job_hash[:job_name]}"
      id, name, args = job_hash[:id].to_i, job_hash[:job_name], job_hash[:args]

      log "Executing job :id => #{id}, :name => #{name}, :args => #{args.inspect}"

      begin
        client.start_job! id

        job = ::BgExecutor::Job.create(id, name, args)
        job.execute

        client.finish_job! id

        log "Finished job ##{id}"
      rescue
        client.fail_job! id, $!

        log "Failed job id => #{id}, :name => #{name}, :args => #{args.inspect}\n\nError: #{$!.message}\n\nBacktrace: #{$!.backtrace.join("\n")}"
      end
      $0 = "Job ##{job_hash[:id]}: #{job_hash[:job_name]}*"
    end

    def execute_regular_job(name, params)
      $0 = "Job_regular: #{name}"
      log "Executing regular job #{name}"
      job = ::BgExecutor::Job::Regular.create(name, params)
      job.execute
      log "Finished regular job #{name}"
      $0 = "Job_regular: #{name}*"
    rescue
      log "Failed regular job #{name}, :params => #{params.inspect} \n\nError: #{$!.message}\n\nBacktrace: #{$!.backtrace.join("\n")}"
    end

    # Log message to stdout
    def log(message)
      puts "%s: %s" % [Time.now.to_s, message]
    end
  end # end class
end # end module