module SchedulingAlgorithm
  class AbstractAlgorithm
    def initialize()
      raise "Cannot directly instantiate an AbstractAlgorithm." if self.class == AbstractAlgorithm
    end
    def schedule_job(job_list, worker_status, current_schedule)
      raise NotImplementedError
      #return {job_id => [worker1, worker2...]}
    end
  end
end
