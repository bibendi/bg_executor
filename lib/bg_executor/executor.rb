module BgExecutor
  class Executor

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
      rescue Exception => e
        log "Failed job id => #{id}, :name => #{name}, :args => #{args.inspect}\n\nError: #{e.message}\n\nBacktrace: #{e.backtrace.join("\n")}"
        client.fail_job! id, e
      end
      $0 = "Job ##{job_hash[:id]}: #{job_hash[:job_name]}*"
    end

    def execute_regular_job(job)
      $0 = "Job: #{job[:name]}"
      log "** Executing regular job #{job[:name]} **"
      ::BgExecutor::Job::Regular.create(job[:name], job[:args]).execute
      log "*** Finished regular job #{job[:name]} **"
      $0 = "Job: #{job[:name]}*"
    rescue Exception => e
      log "Failed regular job #{job[:name]}, :params => #{job[:params].inspect} \n\nError: #{e.message}\n\nBacktrace: #{e.backtrace.join("\n")}"
    end

    # Log message to stdout
    def log(message)
      puts "%s: %s" % [Time.now.to_s, message]
    end
  end # end class
end # end module