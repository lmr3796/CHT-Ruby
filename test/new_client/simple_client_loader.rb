#! /usr/bin/env ruby
require 'logger/colors'

require_relative '../../config/config.rb'
require_relative '../../src/client.rb'
require_relative '../../src/job.rb'

def get_client()
  logger = Logger.new(STDERR)
  logger.level = CHT_Configuration::LOGGER_LEVEL
  dispatcher_addr = CHT_Configuration::Address::DISPATCHER
  dispatcher_uri = CHT_Configuration::Address::druby_uri dispatcher_addr
  return Client.new(dispatcher_uri, logger)
end
def get_job(deadline=Time.now+300)
  j = Job.new
  20.times { j.add_task Task.new('sleep',['1'])}
  j.deadline = deadline
  j.priority = rand(1...20)
  return j
end
c = get_client
c.register
c.start
j1 = get_job
j2 = get_job
j2.priority=100
c.submit_jobs(j1)
sleep rand * 10
c.submit_jobs(j2)
c.submit_jobs([get_job, get_job, get_job])
sleep rand * 10
c.submit_jobs([get_job, get_job, get_job])
sleep rand * 10
c.submit_jobs([get_job, get_job, get_job])
c.submit_jobs([get_job, get_job, get_job])
c.submit_jobs([get_job, get_job, get_job])
c.wait_all
meet_cnt = c.results.select{|j, rl| c.finish_time[j] < c.submitted_jobs[j].deadline}.size
p "#{meet_cnt} out of #{c.results.size} jobs met deadline."
