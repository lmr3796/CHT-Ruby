require_relative 'abstract_algorithm'

module SchedulingAlgorithm
  class WorkloadBasedScheduling
    def initialize()
    end
    def schedule_job(job_list, worker_status, current_schedule)
      # job_list: {job_id => Job instance}
      # worker_status: {worker_id => status}
      # current_schedule: {job_id => [worker_id, ...]}
      # Return: {job_id => [worker_id, ...]}
      # Concept: 
      # Priority represented by smaller number is of higher priority.
      remaining_worker = worker_status.keys
      schedule_result = {}
      # Compute workload for each job
      workload_list = Hash.new{ |hash, key|
        hash[key] = 1.0 * job.task.size * job.task_running_time_on_worker.values.reduce(:+) / job.task_running_time_on_worker.size
      }
      job_id_by_workload = job_list.keys.sort_by{ |job_id| -workload_list[job_id] }
      total_workload = job_list.keys.inject{ |sum, job_id| sum = sum + workload_list[job_id] }
      # Compute the number of worker for each job
      remaining_worker_count = remaining_worker.size
      job_worker_count = {}
      # Assign a worker for each job if possible
      job_id_by_workload.each_with_index{ |job_id, index|
        job_worker_count[job_id] = index < remaining_worker_count ? 1 : 0
      }
      remaining_worker_count = remaining_worker_count - [remaining_worker_count, job_id_by_workload.size].min
      # Assign rest workers according to the amount of workload
      expected_worker_count = Hash.new{ |hash, key|
        hash[key] = remaining_worker_count * workload_list[key] / total_workload
      }
      job_id_by_workload.each{ |job_id|
        break if remaining_worker_count == 0
        additional_worker_count = [expected_worker_count[job_id].floor, job_list[job_id].task.size - job_worker_count[job_id], remaining_worker_count].min
        remaining_worker_count -= additional_worker_count
        job_worker_count[job_id] += additional_worker_count
      }
      # If there are workers left, assign them to work with higher workload
      job_id_by_workload.each{ |job_id|
        break if remaining_worker_count == 0
        additional_worker_count = [job_list[job_id].task.size - job_worker_count[job_id], remaining_worker_count].min
        remaining_worker_count -= additional_worker_count
        job_worker_count[job_id] += additional_worker_count
      }
      # Assign a worker for each job
      job_id_by_workload.each{ |job_id|
        job = job_list[job_id]
        worker_by_throughput = remaining_worker.sort_by{ |worker_id| job.task_running_time_on_worker[worker_id] }
        schedule_result[job_id] = worker_by_throughput.slice!(0, job_worker_count[job_id])
        remaining_worker = worker_by_throughput
      }
    end
  end
end
