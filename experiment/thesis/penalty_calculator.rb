#! /usr/bin/env ruby

require_relative 'workload_parser'

def penalty(job, submit_time, finish_time)
  return [0, job.priority * (finish_time - job.deadline)].max
end

penalty_of_each_file = Hash[ARGV.map do |f|
  result = Marshal.load(open(f).read)
  jobs = result[:jobs]
  submit_time = result[:submit_time]
  finish_time = result[:finish_time]
  each_penalty = jobs.each_key.map do |j_id|
    penalty(jobs[j_id], submit_time[j_id], finish_time[j_id])
  end
  next [f, each_penalty.reduce(:+)]
end]
penalty_of_each_file.each do |f,p|
  puts "#{f}:\t#{p}"
end
puts ''
penalty_of_each_file.each{|f,p|print "#{p}\t"}
puts ''
puts ''
puts "Average: #{penalty_of_each_file.values.reduce(:+) / penalty_of_each_file.size}"
