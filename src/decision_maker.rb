require_relative 'base_server'

class DecisionMaker < BaseServer
  attr_writer :status_checker, :algorithm
  def initialize(alg, status_checker=nil, arg={})
    super arg[:logger]
    @algorithm = alg
    @status_checker = status_checker
  end
  def schedule_job(job_list, worker_status, arg={})
    @logger.info "Rescheduling on #{job_list.keys}"
    # Possibly something to be done with arg in the future :)
    result = @algorithm.schedule_job job_list, worker_status, arg.merge({:logger => @logger}) # Specify logger for algorithms to use
    result.merge!(Hash[job_list.keys.map{|k|[k,[]]}]){|k,old,new| old}  # Still retain a empty array even when no node scheduled
    @logger.info "Scheduled result: #{result}"
    return result
  end
end
