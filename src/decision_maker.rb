class DecisionMaker
  attr_writer :status_checker, :algorithm
  def initialize(alg, status_checker=nil)
    @algorithm = alg
    @status_checker = status_checker
  end
  def schedule_job(job_list, worker_status, current_schedule, arg={})
    # Possibly something to be done with arg in the future :)
    return @algortihm.schedule_job job_list, worker_status, current_schedule
  end
end
