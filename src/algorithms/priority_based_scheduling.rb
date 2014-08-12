require_relative 'abstract_algorithm'

module SchedulingAlgorithm
  class PriorityBasedScheduling < AbstractAlgorithm
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
      # Assign a worker for each job
      job_id_by_priority.each{ |job_id|
        break if remaining_worker.empty?
        job = job_list[job_id]
        best_worker_index = (0...remaining_worker.size).min_by{ |index| job.task_running_time_on_worker[remaining_worker[index]] }
        schedule_result[job_id] = [remaining_worker[best_worker_index]]
        remaining_worker.delete_at(best_worker_index)
      }
      # Assign as more worker as we can for job with high priority
      job_id_by_priority.each{ |job_id|
        break if remaining_worker.empty?
        job = job_list[job_id]
        worker_by_throughput = remaining_worker.sort_by{ |worker_id| job.task_running_time_on_worker[worker_id] }
        worker_needed = [worker_by_throughput.size, job.progress.undone.size].min
        schedule_result[job_id] << worker_by_throughput.slice!(0, worker_needed)
        schedule_result[job_id].flatten!
        remaining_worker = worker_by_throughput
      }
      return schedule_result
    end
  end
end
