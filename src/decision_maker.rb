require_relative 'base_server'

class DecisionMaker < BaseServer
  attr_writer :status_checker, :algorithm
  def initialize(alg, status_checker=nil, arg={})
    super arg[:logger]
    self.algorithm = alg
    @status_checker = status_checker
    return
  end

  def algorithm=(alg)
    @algorithm = alg
    @logger.info "Scheduling policy set to #{alg.class}"
  end

  def schedule_job(job_list, worker_status, arg={})
    @logger.info "Rescheduling on #{job_list.keys}"

    # TODO: Possibly something to be done with arg in the future :)
    # Specify logger for algorithms to use
    result = @algorithm.schedule_job(job_list, worker_status, arg.merge({:logger => @logger}))
    # Still retain a empty array even when no node scheduled
    result.merge!(Hash[job_list.keys.map{|k|[k,[]]}]){|k,old,new| old}
    @logger.info "Scheduled result: #{result}"
    return result
  rescue => e
    @logger.error e.message
    @logger.error e.backtrace.join("\n")
    system('killall ruby')
  end
end
