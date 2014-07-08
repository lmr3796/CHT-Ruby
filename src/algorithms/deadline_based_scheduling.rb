require 'time'

require_relative 'abstract_algorithm'

module SchedulingAlgorithm
  class DeadlineBasedScheduling
    def initialize()
    end
    def schedule_job(job_list, worker_status, arg={})
      # job_list: {job_id => Job instance}
      # worker_status: {worker_id => status}
      # current_schedule: {job_id => [worker_id, ...]}
      # Return: {job_id => [worker_id, ...]}
      # Concept: make jobs with higher priority meet their deadlines
      # Priority represented by smaller number is of higher priority.

      @logger = arg[:logger]
      current_timestamp = Time.now  # Should be consistent within the whole schedule process
      job_running_time = arg[:job_running_time]
      worker_avg_running_time = arg[:job_running_time]
      job_id_by_priority = job_list.keys.sort_by{ |job_id| job_list[job_id].priority }
      remaining_worker = worker_status.keys
      schedule_result = {}

      job_id_by_priority.each{ |job_id|
        break if remaining_worker.empty?
        job = job_list[job_id]
        @logger.debug "job_id, task_remaining = #{job_id}, #{job.task_remaining}"
        history = job_running_time[job_id]
        avg_time = history.reduce(:+)/history.size rescue nil
        job.avg_task_running_time = avg_time if avg_time != nil && avg_time > 0
        worker_by_throughput = remaining_worker.sort_by{|worker_id| job.task_running_time_on_worker[worker_id]}
        assigned_worker_offset, assigned_worker_size = get_required_worker_range(job_id, job, worker_by_throughput, current_timestamp,
                                                                                 job_running_time, worker_avg_running_time)
        schedule_result[job_id] = worker_by_throughput.slice!(assigned_worker_offset, assigned_worker_size)
        remaining_worker = worker_by_throughput
      }
      return schedule_result
    end

    def get_required_worker_range(job_id, job, worker_by_throughput, current_timestamp, job_running_time, worker_avg_running_time)
      if worker_by_throughput.size == 0
        @logger.info "No more worker available to schedule"
        return 0, 0
      end

      @logger.info "Deadline of #{job_id} is #{job.deadline} and now is #{current_timestamp}"
      # If the deadline is already passed, assign as many as workers for the job.
      if current_timestamp > job.deadline
        @logger.warn "#{job_id} missed deadline; give as much as possble"
        return 0, [worker_by_throughput.size, job.task_remaining].min
      end

      # Compute the range of worker required to make the job meet its deadline
      needed_worker = 0
      total_throughput = 0.0
      required_throughput = job.task_remaining / Float(job.deadline-current_timestamp)
      worker_by_throughput.each_with_index do |worker_id|
        break if total_throughput > required_throughput
        break if needed_worker == job.task_remaining
        needed_worker += 1

        # Try to estimate run time then get workers to occupy
        if job.task_running_time_on_worker[worker_id] != nil
          estimated_running_time = job.task_running_time_on_worker[worker_id]
          @logger.info "Estimate run time using provided runtime = #{estimated_running_time} sec."
        elsif job.avg_task_running_time != nil && job.avg_task_running_time > 0
          @logger.info "Estimate run time using average task execution time = #{job.avg_task_running_time} sec."
          estimated_running_time = job.avg_task_running_time
        else
          all = worker_avg_running_time.values.flatten
          avg = all / all.size rescue nil
          if avg != nil && avg > 0
            estimated_running_time = avg
            @logger.info "Estimate run time using system average = #{estimated_running_time} sec."
          else
            estimated_running_time = INTIAL_EXEC_TIME_GUESS
            @logger.info "Estimate run time using default value = #{estimated_running_time} sec."
          end
        end
        total_throughput += 1.0 / estimated_running_time
      end
      return 0, needed_worker
    end

    private :get_required_worker_range
  end
end
