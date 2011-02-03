class DemandJob < BgExecutor::Job

  acts_as_singleton [:users]

  def execute
    raise "users parameter expected" if params[:users].nil?
    puts 'DemandJob work!'
  end
end
