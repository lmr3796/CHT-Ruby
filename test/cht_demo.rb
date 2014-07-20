#! /usr/bin/env ruby

require 'time'
require 'thread'

require_relative '../config/config'
require_relative '../src/client'
require_relative '../src/job'


RUN_PATH='$HOME'+'/CHT-Ruby/test/spec_job_scripts'
BZIP2_PER_TASK_RUNNING_TIME = 47
H264_PER_TASK_RUNNING_TIME = 32
WC_PER_TASK_RUNNING_TIME = 21


total_deadline = Time.now + ARGV.shift.to_f
delay = ARGV.shift.to_f

wc_job = Job.new('word count')
bzip2_job = Job.new('bzip2')
h264_job = Job.new('h264')
for i in 0...3 do
  wc_job.add_task Task.new(RUN_PATH + '/word_count.sh test')
end
for i in 0...10 do
  bzip2_job.add_task Task.new(RUN_PATH + '/bzip2.sh test')
end
for i in 0...4 do
  h264_job.add_task Task.new(RUN_PATH + '/h264.sh test')
end

CHT_Configuration::Address::WORKERS.keys.each do |worker|
  wc_job.task_running_time_on_worker[worker] = WC_PER_TASK_RUNNING_TIME
  bzip2_job.task_running_time_on_worker[worker] = BZIP2_PER_TASK_RUNNING_TIME
  h264_job.task_running_time_on_worker[worker] = H264_PER_TASK_RUNNING_TIME
end

wc_job.priority = 3
bzip2_job.priority = 2
h264_job.priority = 1

# First batch
puts 'Dispatching first batch'
job_set = [wc_job]
job_set.each {|job| job.deadline = total_deadline}
# TODO: print schedule?
#print schedule
dispatcher_addr = CHT_Configuration::Address::DISPATCHER
c1 = Client.new CHT_Configuration::Address::druby_uri(dispatcher_addr), job_set
c1.start()

## Simulate gap between job arrival
print "Sleep for #{delay} seconds..."
sleep(delay)
puts "go"

# Second batch
puts 'Dispatching second batch'
job_set = [bzip2_job, h264_job]
job_set.each do |job|
  job.deadline = total_deadline - delay
end
# TODO: print schedule?
dispatcher_addr = CHT_Configuration::Address::DISPATCHER
c2 = Client.new CHT_Configuration::Address::druby_uri(dispatcher_addr), job_set
c2.start()

# Wait till finish
c2.wait_all
c1.wait_all

