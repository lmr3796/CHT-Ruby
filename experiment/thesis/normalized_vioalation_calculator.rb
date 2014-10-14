#! /usr/bin/env ruby

require 'set'
require 'time'

vio = []
input_file = open(ARGV.shift) rescue $stdin
input_file.each_line do |line|
  case line
  when /Rescheduling on/ 
    #vio.delete_at(-1) if !vio.empty? && vio.last[1].size == 0
    timestamp, job_set = line.scan(/\[.*?\]/)
    timestamp = Time.parse(timestamp[1...-1].split[0])
    job_set = eval(job_set)
    vio << [job_set.size, Set.new] if job_set.size > 1
  when /violated/
    splitted_line = line.split
    #vio.last[1] << [splitted_line[-4], splitted_line[-1]]
    vio.last[1] << splitted_line[-4]
  end
end
vio.map! do |job_set_size, violation_set| 
  #Float(violation_set.size) / (job_set_size * (job_set_size - 1) / 2)
  Float(violation_set.size) / job_set_size
end
rate = vio.reduce(:+) / vio.size rescue 0
puts rate
