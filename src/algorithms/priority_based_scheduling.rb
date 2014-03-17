require_relative 'abstract_algorithm'

module SchedulingAlgorithm
  class PriorityBasedScheduling
    def initialize()
    end
    def schedule_job(job_list, worker_status, current_schedule)
      # job_list: {job_id => Job instance}
      # worker_status: {worker_id => status}
      # current_schedule: {job_id => [worker_id, ...]}
      # Return: {job_id => [worker_id, ...]}
      # Concept: 
      # Priority represented by smaller number is of higher priority.
      sorted_job_id = job_list.keys.sort{ |job_id| job_list[job_id].priority }
      remaining_worker = worker_status.keys
      schedule_result = {}
      sorted_job_id.each{ |job_id|
        job = job_list[job_id]
        best_worker_index = nil
        best_running_time = nil
        remaining_worker.each_with_index{ |worker_id, index|
          if best_worker_index == nil || best_running_time > job.task_running_time_on_worker[worker_id]
            best_worker_index = index
            best_running_time = job.task_running_time_on_worker[worker_id]
          end
        }
        schedule_result[job_id] = [remaining_worker[best_worker_index]]
        remaining_worker.delete_at(best_worker_index)
      }
      sorted_job_id.each{ |job_id|
        break if remaining_worker.empty?
        job = job_list[job_id]
        worker_by_throughput = remaining_worker.sort_by{ |worker_id| job.task_running_time_on_worker[worker_id] }
        worker_needed = [worker_by_throughput.size, job.task.size - 1].min
        schedule_result[job_id] << worker_by_throughput.slice!(0, worker_needed)
        schedule_result[job_id].flatten!
        remaining_worker = worker_by_throughput
      }
      return schedule_result
    end
  end
end
