module JobsHelper
  def progress_bar(job_id, secure_key, options = {})
    client = extract_client_from options

    job_exists = begin
      client.job_exists? job_id, secure_key
    rescue
      false
    end

    return render :partial => 'progress_bar/not_found', :locals => {:job_id => job_id} unless job_exists

    # extract options
    options[:interval]    ||= 1.second
    options[:success_url] ||= url_for()
    options[:error_url]   = url_for() unless options.has_key? :error_url 
    options[:status_messages]            ||= {}
    options[:status_messages][:new]      ||= "Preparing for execution..."
    options[:status_messages][:running]  ||= "Processing..."
    options[:status_messages][:failed]   ||= "An error occured..."
    options[:status_messages][:finished] ||= "Done"

    render :partial => 'progress_bar/bar', :locals => {:job_id => job_id,
                                                       :secure_key => secure_key,
                                                       :options => options}
  end

  private
  def extract_client_from(options)
    raise ArgumentError, 'Hash expected' unless options.is_a?(Hash)

    client = options[:client] || options[:bg_executor_client] || ::BgExecutor::Client.new

    raise ArgumentError, "BgExecutor::Client expected, #{client.class.name} given instead" unless client.is_a?(::BgExecutor::Client)
    client
  end
end