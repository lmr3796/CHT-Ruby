require_relative 'abstract_algorithm'

module SchedulingAlgorithm
  class EarliestDeadlineFirstScheduling < AbstractAlgorithm
    def schedule_job(job_list, worker_status, arg={})
      # job_list: {job_id => Job instance}
      # worker_status: {worker_id => status}
      # current_schedule: {job_id => [worker_id, ...]}
      # Return: {job_id => [worker_id, ...]}
      # Concept: Execute jobs with earliest deadline first
      # Priority represented by larger number is of higher priority.
      job_id_by_deadline = job_list.keys.sort_by do |job_id|
        job = job_list[job_id]
        next [job.deadline, job.priority]
      end
      remaining_worker = worker_status.keys
      schedule_result = Hash[job_list.each_pair.map{|job_id, job|[job_id,Array.new]}]

      # Assign as more worker as we can for job with high priority
      job_id_by_deadline.each{ |job_id|
        break if remaining_worker.empty?
        job = job_list[job_id]
        worker_by_throughput = remaining_worker.sort_by{ |worker_id| job.task_running_time_on_worker[worker_id] }
        worker_needed = [worker_by_throughput.size, job.progress.undone.size].min
        schedule_result[job_id] += worker_by_throughput.slice!(0, worker_needed)
        remaining_worker = worker_by_throughput
      }
      return schedule_result
    end
  end
end
