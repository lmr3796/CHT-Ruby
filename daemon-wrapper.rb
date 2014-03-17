#! /usr/bin/env ruby
script_name = ARGV.shift
begin
  require 'daemons'
rescue LoadError
  puts 'Required gem "daemons". Use `gem install daemons` to install'
  exit false
end
Daemons.run(script_name)
