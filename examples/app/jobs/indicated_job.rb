class IndicatedJob < BgExecutor::Job::Indicated
  def execute
    self.total = 10
    
    10.times do
      increment_completed!
    end
    
    puts '1234567890'
  end
end
