require_relative 'base_server'

class DecisionMaker < BaseServer
  attr_writer :status_checker, :algorithm
  def initialize(alg, status_checker=nil, arg={})
    super arg[:logger]
    @algorithm = alg
    @status_checker = status_checker
  end
  def schedule_job(job_list, worker_status, arg={})
    @logger.info 'Rescheduling'
    # Possibly something to be done with arg in the future :)
    result = @algortihm.schedule_job job_list, worker_status, :current_schedule => arg[:current_schedule]
    @logger.info "Result: #{result}"
    return result
  end
end
