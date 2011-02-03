module BgExecutor
  class Configuration
    class << self
      def read_config(config_file)
        require 'yaml' unless defined? YAML
        require 'erb'  unless defined? ERB

        raise ConfigurationFileMissing unless File.exists?(config_file)

        config = YAML.load(ERB.new(IO.read(config_file)).result)
        environment = ENV["RAILS_ENV"] || "development"
        environment = config[:bg_executor][:environment] if config[:bg_executor] && config[:bg_executor][:environment]

        if respond_to?(:silence_warnings)
          silence_warnings do
            Object.const_set("RAILS_ENV",environment)
          end
        else
          Object.const_set("RAILS_ENV",environment)
        end

        ENV["RAILS_ENV"] = environment
        config
      end

      def [](key)
        config.has_key?(key.to_sym) ? config[key.to_sym].symbolize_keys : nil
      end

      def config
        @config ||= read_config(config_file).symbolize_keys
      end

      def config_file
        if File.exists?("#{RAILS_ROOT}/config/bg_executor.local.yml")
          "#{RAILS_ROOT}/config/bg_executor.local.yml"
        else
          "#{RAILS_ROOT}/config/bg_executor.yml"
        end
      end
    end
  end
end