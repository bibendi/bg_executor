ActiveRecord::Base.logger.level = Logger::Severity::UNKNOWN
ActiveRecord::Base.logger = Logger.new('/dev/null')

Daemons::Application.class_eval do
  def exception_log
    #stub
  end
end

daemon = BgExecutor::Daemon.new

$running = true

def terminate
  puts "Start terminating..."
  $running = false
end

Signal.trap("TERM"){ terminate }
Signal.trap("KILL"){ terminate }
Signal.trap("INT") { terminate }

while($running) do
  daemon.execute_job
  daemon.execute_regular_jobs
  sleep 0.5
end

puts 'Exit'