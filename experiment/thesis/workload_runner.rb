#! /usr/bin/env ruby

require 'optparse'
require 'time'

require_relative 'workload_parser.rb'

# Option parsing
OptionParser.new do |opts|
  $options={}
  # This displays the help screen, all programs are
  # assumed to have this option.
  opts.on( '-h', '--help', 'Display this screen' ){puts opts; exit}
  opts.on( '-r rate', '--sample-rate rate', Float, 'Set job sampling rate' ) do |r|
    $options[:job_sample_rate] = r
  end
  opts.on( '-d rate', '--deadline-rate rate', Float, 'Set deadline (ratio to runtime)' ) do |r|
    $options[:deadline_rate] = r
  end
  opts.on( '-c rate', '--scale-rate rate', Float, 'Set job scale rate' ) do |r|
    $options[:job_scale_rate] = r 
  end
  opts.on( '-t lower,upper', '--runtime-limit lower,upper', Array, 'Set job runtime limit' ) do |r|
    $options[:job_time_limit] = Range.new r[0].to_f, r[1].to_f
  end
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
runner = WorkloadSynthesizer.new jobs, $options
jobs = runner.job_set_to_run
puts jobs.sample(10)
puts runner.estimated_cpu_time
