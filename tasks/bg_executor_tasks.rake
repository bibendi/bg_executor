namespace :bg_executor do
  require 'yaml'

  desc 'Setup bg_executor in your Rails application'
  task :setup do
    script_dest = "#{RAILS_ROOT}/script/bg_executor.rb"
    script_src = File.dirname(__FILE__) + "/../script/bg_executor.rb"

    FileUtils.chmod 0774, script_src

    defaults = {"redis" => {"host" => '127.0.0.1', "port" => 15822, "namespace" => "production"},
                "bg_executore" => {"concurrency" => 4, "queue_timeout" => 300}}

    config_dest = "#{RAILS_ROOT}/config/bg_executor.yml"

    unless File.exists?(config_dest)
        puts "Copying bg_executor.yml config file to #{config_dest}"
        File.open(config_dest, 'w') { |f| f.write(YAML.dump(defaults)) }
    end

    unless File.exists?(script_dest)
        puts "Copying bg_executor script to #{script_dest}"
        FileUtils.cp_r(script_src, script_dest)
    end

    jobs_dest = "#{RAILS_ROOT}/app/jobs"
    unless File.exists?(jobs_dest)
      puts "Creating #{jobs_dest}"
      FileUtils.mkdir(jobs_dest)
    end
  end

  desc 'Remove bg_executor from your Rails application'
  task :remove do
    script_src = "#{RAILS_ROOT}/script/bg_executor.rb"
    config_src = "#{RAILS_ROOT}/config/bg_executor.yml"
    config_local_src = "#{RAILS_ROOT}/config/bg_executor.local.yml"
    files = [script_src, config_src, config_local_src]

    files.each do |src|
      if File.exists?(src)
          puts "Removing #{src} ..."
          FileUtils.rm(src, :force => true)
      end
    end

    jobs_dest = "#{RAILS_ROOT}/app/jobs"
    if File.exists?(jobs_dest) && Dir.entries("#{jobs_dest}").size == 2
        puts "#{jobs_dest} is empty...deleting!"
        FileUtils.rmdir(jobs_dest)
    end
  end

  desc 'See all jobs'
  task :jobs => :environment do
    client = BgExecutor::Client.new
    jobs = client.all_jobs

    jobs.each do |job|
      job.symbolize_keys!
      puts "id: %-5d\tjob: %-45s\tstatus: %-11s\texecution_time: %-.2f" % [job[:id], job[:job], job[:status], job[:execution_time]]
    end
  end
end