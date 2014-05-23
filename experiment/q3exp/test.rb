#! /usr/bin/env ruby

require_relative './workload-generator.rb'
require_relative '../../config/config'
require_relative '../../src/client'
require_relative '../../src/job'

DEFAULT_PRIORITY = 1

begin
  file = open(ARGV.shift)
rescue
  puts "Can't open workload log."
  exit(-1)
end

deadlines = ARGV.map {|i| Float(i) rescue nil}
if deadlines.size == 0 or deadlines.select{|d| d == nil or !d.is_a? Float}.size > 0
  puts "Can't parse deadlines from ARGV."
  exit(-1)
end

workload = WorkloadParser::workload_from_file file 
jobs = workload.map do |i|
  j = Job.new i[:name]
  j.priority = DEFAULT_PRIORITY
  i[:task].each do |t|
    sleep_time = t.values.reduce(:+)
    j.add_task(Task.new("sleep #{sleep_time}"))
  end
  j
end

if jobs.size != deadlines.size
  puts "Deadline provided doesn't match #jobs"
  exit(-1)
end

jobs.zip(deadlines).each do |j, d|
  j.deadline = d
end

client = Client.new CHT_Configuration::Address::druby_uri(CHT_Configuration::Address::DISPATCHER), jobs
client.start
client.wait_all
