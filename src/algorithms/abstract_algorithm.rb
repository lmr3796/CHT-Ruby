module SchedulingAlgorithm
  class AbstractAlgorithm
    def initialize()
      raise "Cannot directly instantiate an AbstractAlgorithm." if self.class == AbstractAlgorithm
    end
    def schedule_job(job_list, worker_status)
      raise NotImplementedError
    end
  end
end
