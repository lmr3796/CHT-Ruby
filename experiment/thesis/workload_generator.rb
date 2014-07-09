#! /usr/bin/env ruby

require 'logger/colors'
require 'optparse'
require 'time'

require_relative 'workload_parser.rb'

# Default options
$options = {
  :job_sample_rate => 1.0,
  :deadline_rate => 2.0,
  :job_scale_rate => 1.0,
  :wait_time_scale_rate => 1.0,
  :batch_threshold => 1.0,
}

# Option parsing
OptionParser.new do |opts|
  # This displays the help screen, all programs are
  # assumed to have this option.
  opts.on( "-h", "--help", "Display this screen"){puts opts; exit}
  opts.on("-s rate", "--sample-rate rate", Float, "Set job sampling rate. #{$options[:job_sample_rate]} by default") do |r|
    $options[:job_sample_rate] = r
  end
  opts.on("-d rate", "--deadline-rate rate", Float, "Set deadline (ratio to runtime). #{$options[:deadline_rate]} by default") do |r|
    $options[:deadline_rate] = r
  end
  opts.on("-c rate", "--cpu-scale-rate rate", Float, "Set job scale rate. #{$options[:job_scale_rate]} by default") do |r|
    $options[:job_scale_rate] = r
  end
  opts.on("-w rate", "--wait-time-scale-rate rate", Float, "Set waiting time scale rate. #{$options[:wait_time_scale_rate]} by default") do |r|
    $options[:wait_time_scale_rate] = r
  end
  opts.on("-b time", "--batching-threshold time", Float, "Set the threshold to batch jobs together. #{$options[:batch_threshold]} by default") do |r|
    $options[:batch_threshold] = r
  end
  #opts.on( '-t lower,upper', '--run-time-limit lower,upper', Array, 'Set job run time limit' ) do |r|
  #  $options[:job_exec_time_limit] = Range.new r[0].to_f, r[1].to_f
  #end
  #opts.on( '-T limit', '--wait-time-limit limit', Float, 'Set job wait time limit' ) do |r|
  #  $options[:job_wait_time_limit] = r
  #end

  opts.on( '-i', '--input file_name', 'Set workload file name' ) do |f|
    begin
      file = open f
      $options[:input] = file
    rescue
      puts "Can't open input file #{f}"
      exit(-1)
    end
  end

  opts.on( '-o', '--output file_name', 'Set output file name' ) do |f|
    begin
      file = open(f, 'w')
      $options[:output] = file
    rescue
      puts "Can't open output file #{f}"
      exit(-1)
    end
  end
end.parse!

$options[:input] ||= $stdin
$options[:output] ||= $stdout

$stderr.puts "Parse from swf input."
jobs = StandardWorkloadFormatParser.from_file $options[:input]
runner = WorkloadSynthesizer.new(jobs, $options)
batch = runner.gen_workload

dump = Marshal.dump([jobs, batch])
raise "Output failed." if $options[:output].write(dump) != dump.size
exit
