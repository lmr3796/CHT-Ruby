require_relative 'abstract_algorithm'

module SchedulingAlgorithm
  class DeadlineBasedScheduling
    def initialize()
    end
    def schedule_job(job_list, worker_status, current_schedule)
      # job_list: {job_id => Job instance}
      # worker_status: {worker_id => status}
      # current_schedule: {job_id => [worker_id, ...]}
      # Return: {job_id => [worker_id, ...]}
      # Concept: make jobs with higher priority meet their deadlines
      # Priority represented by smaller number is of higher priority.
      sorted_job_id = job_list.keys.sort{ |job_id| job_list[job_id].priority }
      remaining_worker = worker_status.keys
      schedule_result = {}
      sorted_job_id.each{ |job_id|
        job = job_list[job_id]
        worker_by_throughput = remainging_worker.sort_by{ |worker_id| job.task_running_time_on_worker[worker_id]}
        assigned_worker_offset, assigned_worker_size = get_required_worker_range(job, worker_by_throughput)
        schedule_result[job_id] = worker_by_throughput.slice!(assigned_worker_offset, assigned_worker_size)
        remaining_worker = worker_by_throughput
      }
      return schedule_result
    end

    def get_required_worker_range(job, worker_by_throughput)
      needed_worker = 0
      total_throughput = 0.0
      required_throughput = job.task.size * 1.0 / job.deadline
      worker_by_throughput.each{ |worker_id|
        break if total_throughput > required_throughput
        break if needed_worker == job.task.size
        needed_worker += 1
        total_throughput += 1.0 / job.task_running_time_on_worker[worker_id]
      }
      return 0, needed_worker
    end

    private :get_required_worker_range
  end
end
