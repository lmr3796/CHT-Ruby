#! /usr/bin/env ruby
require 'logger/colors'

require_relative '../config/config.rb'
require_relative '../src/client.rb'
require_relative '../src/job.rb'

def get_client()
  dispatcher_addr = CHT_Configuration::Address::DISPATCHER
  dispatcher_uri = CHT_Configuration::Address::druby_uri dispatcher_addr
  return Client.new dispatcher_uri
end
def get_job(deadline=Time.now+300)
  j = Job.new
  3.times { j.add_task Task.new('sleep',['10'])}
  j.deadline = deadline
  return j
end
c = get_client
c.start
c.submit_jobs([get_job, get_job])
c.wait_all
