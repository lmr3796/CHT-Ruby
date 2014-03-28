#! /usr/bin/env ruby

require 'thread'

require_relative '../config/config'
require_relative '../src/job'



BZIP2_PER_TASK_RUNNING_TIME = 42
H264_PER_TASK_RUNNING_TIME = 80
WC_PER_TASK_RUNNING_TIME = 14


total_deadline = ARGV.shift.to_f
delay = ARGV.shift.to_f

wc_job = Job('word count')
bzip2_job = Job('bzip2')
h264_job = Job('h264')
for i in 0...20 do
  wc_job.add_task Task.new(RUN_PATH + '/word_count.sh')
end
for i in 0...10 do
  bzip2_job.add_task Task.new(RUN_PATH + '/bzip2.sh')
end
for i in 0...4 do
  h264_job.add_task Task.new(RUN_PATH + '/h264.sh')
end

CHT_Configuration::Address::WORKERS.each do
  wc_job.task_running_time_on_server[worker] = WC_PER_TASK_RUNNING_TIME
  bzip2_job.task_running_time_on_server[worker] = BZIP2_PER_TASK_RUNNING_TIME
  h264_job.task_running_time_on_server[worker] = H264_PER_TASK_RUNNING_TIME
end

wc_job.priority = 3
bzip2_job.priority = 2
h264_job.priority = 1

# First batch
print 'Dispatching first batch'
job_set = [wc_job]
job_set.each do
  j.deadline = total_deadline
end
# TODO: print schedule?
#schedule = framework.get_dispatcher().schedule_jobs(job_set)
#print schedule
dispatcher_addr = CHT_Configuration::Address::DISPATCHER
c1 = Client.new CHT_Configuration::Address::druby_uri(dispatcher_addr), job_set
c1.start()

# Simulate gap between job arrival
time.sleep(delay)

# Second batch
print 'Dispatching second batch'
job_set = [bzip2_job, h264_job]
job_set.each do
  j.deadline = total_deadline - delay
end
# TODO: print schedule?
#schedule = framework.get_dispatcher().schedule_jobs(job_set)
#print schedule
dispatcher_addr = CHT_Configuration::Address::DISPATCHER
c2 = Client.new CHT_Configuration::Address::druby_uri(dispatcher_addr), job_set
c2.start()

# Wait till finish
c1.wait_all
c2.wait_all

