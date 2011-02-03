module BgExecutor
  class Error < Exception;  end

  class ConfigurationError < Error; end
  class ConfigurationFileMissing < ConfigurationError; end
  class ConfigurationParameterMissing < ConfigurationError; end
  class ConnectionError < Error; end
  class QueueError < Error; end
  class JobExecutionError < Error; end
  class JobNotFound < Error; end
  class JobAccessError < Error; end
end