require_relative 'abstract_algorithm'

module SchedulingAlgorithm
  class PriorityBasedScheduling < AbstractAlgorithm
    PRESERVE_RATE=0.3
    def initialize()
    end
    def schedule_job(job_list, worker_status, arg={})
      # job_list: {job_id => Job instance}
      # worker_status: {worker_id => status}
      # current_schedule: {job_id => [worker_id, ...]}
      # Return: {job_id => [worker_id, ...]}
      # Concept:
      # Priority represented by larger number is of higher priority.
      job_id_by_priority = job_list.keys.sort_by{ |job_id| job_list[job_id].priority }.reverse
      remaining_worker = worker_status.keys
      schedule_result = {}
      logger = arg[:logger]

      return schedule_result if job_list.empty?

      # Assign as more worker as we can for job with high priority
      job = job_list[job_id_by_priority[0]]
      worker_by_throughput = remaining_worker.sort_by{ |worker_id| job.task_running_time_on_worker[worker_id] }
      worker_needed = [(worker_by_throughput.size * (1 - PRESERVE_RATE)).round, job.progress.undone.size].min
      logger.debug "Unpreserved = #{(worker_by_throughput.size * (1 - PRESERVE_RATE)).round}"
      logger.debug "Undone = #{job.progress.undone.size}"
      logger.debug "Needed = #{worker_needed}"

      schedule_result[job_id_by_priority[0]] = worker_by_throughput.slice!(0, worker_needed)
      remaining_worker = worker_by_throughput


      # Assign a worker for each job
      job_id_by_priority[1..-1].each do |job_id|
        break if remaining_worker.empty?
        job = job_list[job_id]
        best_worker_index = (0..-1).min_by{|i| job.task_running_time_on_worker[remaining_worker[i]]}
        schedule_result[job_id] = [remaining_worker[best_worker_index]]
        remaining_worker.delete_at(best_worker_index)
      end
      return schedule_result
    end
  end
end
