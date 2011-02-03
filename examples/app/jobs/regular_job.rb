class RegularJob < BgExecutor::Job::Regular

  def execute
    puts Time.now
  end
end
