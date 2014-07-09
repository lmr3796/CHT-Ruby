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
  opts.on('--dry-run', "Don't submit jobs, give job info"){$options[:dry_run]=true}

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

if(f = ARGV.shift)
  raise 'Extra argument specified' if $options[:input]
  begin
    $options[:input] = open(f)
  rescue
    puts "Can't open input file #{f}"
    exit(-1)
  end
end
$options[:input] ||= $stdin
$options[:output] ||= $stdout

$options[:logger] = Logger.new(STDERR)
$stderr.puts "Deserialize from input."
jobs, batch = Marshal.load($options[:input].read) 
runner = WorkloadSynthesizer.new jobs, $options
$stderr.puts "Total #{batch.size} batches, #{batch.map{|b| b[:batch].size}.reduce(:+)} jobs to simulate"
exit if !!$options[:dry_run]
jobs, finish_time = runner.run(batch)
$options[:output].puts "#{finish_time.select{|j, t| jobs[j][:deadline] >= t}.size} out of #{jobs.size} jobs met deadline."
