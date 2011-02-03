class RegularJob < BgExecutor::Job::Regular
  acts_as_singleton

  def execute
    puts Time.now
  end
end
