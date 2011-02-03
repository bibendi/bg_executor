# Allow the metal piece to run in isolation
require(File.dirname(__FILE__) + "/../../config/environment") unless defined?(Rails)
require 'json'

class JobsMetal
  class << self
    def call(env)
      if env['PATH_INFO'] =~ /^\/__job/
        # parse parameters
        begin
          params = Rack::Utils.parse_query(env["QUERY_STRING"].to_s)
          params = {} unless params.is_a?(Hash)
          params.symbolize_keys!

          action = params[:a]
          secure_key = params[:s]
          return fail if action.blank?
          return fail if secure_key.blank?

          return fail unless ["status", "info", "result", "status_info"].include?(action.to_s)

          job_id = params[:id].to_i

          return fail if job_id.blank?

          begin
            result = client.send("ask_#{action}".to_sym, job_id, secure_key) unless action.to_s == "status_info" # client.ask_status for example
            result = {
                    "status" => client.ask_status(job_id, secure_key),
                    "info"   => client.ask_info(job_id, secure_key)
                    } if action.to_s == "status_info"
            success({"result" => result}.to_json)
          rescue
            msg = RAILS_ENV == "development" ? $!.message : "An error occured"
            success({"error" => msg}.to_json)
          end
        rescue
          fail
        end
      else
        skip
      end
    end

    def fail
      [200, {"Content-Type" => "text/plain"}, ['']]
    end

    def skip
      [404, {"Content-Type" => "text/html"}, ["Not Found"]]
    end

    def success(response)
      [200, {"Content-Type" => "application/json; charset=utf-8"}, [response]]
    end

    protected
    # get BgExecutor client object
    # @return BgExecutor::Client
    def client
      @client ||= BgExecutor::Client.new
    end
  end
end