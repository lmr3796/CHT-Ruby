require_relative '../worker'
module SchedulingAlgorithm
  INTIAL_EXEC_TIME_GUESS = 20.0
  USABLE_WORKER_STATUS = [
    Worker::STATUS::OCCUPIED,
    Worker::STATUS::AVAILABLE,
    Worker::STATUS::BUSY,
  ]
  class AbstractAlgorithm
    def initialize()
      raise "Cannot directly instantiate an AbstractAlgorithm." if self.class == AbstractAlgorithm
      @average_exec_time = INTIAL_EXEC_TIME_GUESS
    end
    def schedule_job(job_list, worker_status, arg={})
      # job_list: {job_id => Job instance}
      # worker_status: {worker_id => status}
      # current_schedule: {job_id => [worker_id, ...]}
      # Return: {job_id => [worker_id, ...]}
      raise NotImplementedError
    end
  end
end
