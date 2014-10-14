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

class ViolationCheckingDecisionMaker < DecisionMaker
  def schedule_job(job_list, worker_status, arg={})
    result = super(job_list, worker_status, arg)
    log_priority_violation(result, job_list)
    return result
  rescue => e
    @logger.error e.message
    @logger.error e.backtrace.join("\n")
    system('killall ruby')
  end

  def log_priority_violation(result, job_list)
    scheduled = job_list.select{|job_id, job| result[job_id].size > 0}
    unscheduled = job_list.reject{|job_id, job| result[job_id].size > 0 || job.progress.undone.size == 0}
    unscheduled.each do |unsch_job_id, unsch_job|
      scheduled.each do |sch_job_id, sch_job|
        @logger.warn "#{unsch_job_id}(#{unsch_job.priority}) violated by #{sch_job_id}(#{sch_job.priority})" if unsch_job.priority > sch_job.priority
        @logger.warn "#{unsch_job_id} has #{unsch_job.progress.undone.size} undone"
      end
    end
    #job_list.each do |job_id, job|
    #  next if result[job_id].size >= job.progress.undone.size
    #  job_list.each do |jid, j|
    #    next if job_id == jid
    #    @logger.warn "#{job_id}(#{job.priority}) violated by #{jid}(#{j.priority})" if job.priority > j.priority && 
    #      result[job_id].size < result[jid].size
    #  end
    #end
  end
end
