#! /usr/bin/env ruby

require 'optparse'
require 'time'

require_relative 'workload_synthesizer.rb'

# Option parsing
OptionParser.new do |opts|
  $options={}
  # This displays the help screen, all programs are
  # assumed to have this option.
  opts.on( '-h', '--help', 'Display this screen' ){|h| puts opts }
  opts.on( '-r', '--sample-rate rate', Float, 'Set job sampling rate' ){|r| $options[:job_sample_rate] = r }
  opts.on( '-s', '--scale-rate rate', Float, 'Set job scale rate' ){|r| $options[:job_scale_rate] = r }
  opts.on( '-t', '--run-time-limit limit', Integer, 'Set job runtime limit' ){|r| $options[:job_time_limit] = r }
  opts.on( '-f', '--file file_name', 'Set workload file name' ) do |f|
    begin
      file = open f
      $options[:input] = file
    rescue
      puts "Can't open file #{f}"
      exit(-1)
    end
  end
end.parse!
begin
  f = ARGV.shift
  $options[:input] ||= open(f) if f
  $options[:input] ||= STDIN
rescue
  puts "Can't open file #{f}"
  exit(-1)
end

jobs = StandardWorkloadFormatParser.from_file $options[:input]
