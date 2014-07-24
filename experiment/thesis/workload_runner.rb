require 'logger/colors'
require 'optparse'
require 'time'

require_relative 'workload_parser.rb'


module WorkloadRunner
  # Really executes it
  def run(merged_batch, logger=nil)
    total_jobs = jobs_left = merged_batch.map{|b|b[:batch].size}.reduce(:+)
    # Execute
    dispatcher_uri = CHT_Configuration::Address::druby_uri(CHT_Configuration::Address::DISPATCHER)
    if logger == nil
      logger = Logger.new(STDERR)
      logger.level = CHT_Configuration::LOGGER_LEVEL
    end
    client = Client.new(dispatcher_uri, logger)
    client.register
    client.start
    merged_batch.each do |b|
      logger.debug "Sleep for #{b[:wait_time]}"
      sleep b[:wait_time]

      # Convert deadline to real world time
      now = Time.now
      b[:batch].each{|j| j.deadline = now + j.deadline.to_f}

      # Run!!
      client.submit_jobs(b[:batch])
      jobs_left -= b[:batch].size
      logger.warn "Submit #{b[:batch].size} jobs. #{jobs_left}/#{total_jobs} left!"
    end
    client.wait_all
    return {:jobs => client.submitted_jobs.hash_clone,
            :finish_time=>client.finish_time,
            :submit_time=>client.submit_time}
  end
  module_function :run
end
