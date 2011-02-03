#!/usr/bin/env ruby

ENV["RAILS_ENV"] = $1 if ARGV.select{|i| i =~ /^RAILS_ENV=([a-z]+)$/}.first
ENV["RAILS_ENV"] ||= 'development'

require 'rubygems'
require 'active_support'
require "daemons"

require File.expand_path(File.dirname(__FILE__) + '/..') + "/config/environment"

options = {
  :app_name => 'bg_executor_daemon.rb',
  :dir_mode => :normal,
  :dir => RAILS_ROOT + '/log',
  :log_dir => RAILS_ROOT + '/log',
  :log_output => true,
  :multiple => false,
  :backtrace => true,
  :monitor => true
}

Daemons.run(RAILS_ROOT + '/vendor/plugins/bg_executor/lib/bg_executor/bg_executor_daemon.rb', options)
