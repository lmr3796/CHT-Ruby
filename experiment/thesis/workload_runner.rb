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
  opts.on('--dry-run', "Don't submit jobs, give job info"){$options[:dry_run]=true}
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

$options[:logger] = Logger.new(STDERR)
jobs = StandardWorkloadFormatParser.from_file $options[:input]
runner = WorkloadSynthesizer.new jobs, $options
batch = runner.gen_workload
$stderr.puts "Total #{batch.size} batches, #{batch.map{|b| b[:batch].size}.reduce(:+)} jobs to simulate"
exit if !!$options[:dry_run]
jobs, finish_time = runner.run(batch)
p "#{finish_time.select{|j, t| jobs[j][:deadline] >= t}.size} out of #{jobs.size} jobs met deadline."
