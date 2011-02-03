# Include hook code here
%w{ metal views }.each do |code_dir|
  $:.unshift File.join(directory,"app",code_dir)
end
ActionView::Base.send(:include, JobsHelper)

require 'bg_executor'