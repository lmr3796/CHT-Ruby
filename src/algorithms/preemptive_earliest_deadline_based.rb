require 'time'

require_relative 'abstract_algorithm'

module SchedulingAlgorithm
  class PreemptiveNoPriorityDeadlineBasedScheduling
    def schedule_job(job_list, worker_status, arg={})
      # job_list: {job_id => Job instance}
      # worker_status: {worker_id => status}
      # current_schedule: {job_id => [worker_id, ...]}
      # Return: {job_id => [worker_id, ...]}
      # Concept: take deadline as priority
      # Priority represented by larger number is of higher priority.

      @logger = arg[:logger]
      current_timestamp = Time.now  # Should be consistent within the whole schedule process
      job_running_time = arg[:job_running_time]
      worker_avg_running_time = arg[:job_running_time]
      remaining_worker = worker_status.keys
      schedule_result = Hash[job_list.keys.map{|j_id|[j_id,[]]}]

      # Schdule to just enough
      job_list.each_pair.sort_by{|job_id, job| job.deadline}.each do |job_id, job|
        break if remaining_worker.empty?
        history = job_running_time[job_id]
        avg_time = history.reduce(:+)/history.size rescue nil
        job.avg_task_running_time = avg_time if avg_time != nil && avg_time > 0
        worker_by_throughput = remaining_worker.sort_by{|worker_id| job.task_running_time_on_worker[worker_id]}
        assigned_worker_offset, assigned_worker_size = get_required_worker_range(job_id, job, worker_by_throughput, current_timestamp,
                                                                                 job_running_time, worker_avg_running_time)
        begin
          schedule_result[job_id] += worker_by_throughput.slice!(assigned_worker_offset, assigned_worker_size)
        rescue
          @logger.fatal worker_by_throughput
          @logger.fatal assigned_worker_offset
          @logger.fatal assigned_worker_size
          raise
        end
        remaining_worker = worker_by_throughput
      end

      # Aggressively schedule the remaining ones to urgent ones.
      job_list.each_pair.sort_by{|job_id, job| job.deadline}.each do |job_id, job|
        break if remaining_worker.empty?
        next if job.progress.undone.size <= schedule_result[job_id].size
        @logger.debug "Earliest deadline: job_id, undone = #{job_id}, #{job.progress.undone.size}"
        worker_by_throughput = remaining_worker.sort_by{|worker_id| job.task_running_time_on_worker[worker_id]}
        schedule_result[job_id] += worker_by_throughput.slice!(0, job.progress.undone.size - schedule_result[job_id].size)
        remaining_worker = worker_by_throughput
      end

      return schedule_result
    end

    def get_required_worker_range(job_id, job, worker_by_throughput, current_timestamp, job_running_time, worker_avg_running_time)
      if worker_by_throughput.size == 0
        @logger.info "No more worker available to schedule"
        return 0, 0
      end

      @logger.debug "Schduling #{job_id}, prgress:#{job.progress.inspect}"

      @logger.debug "Deadline of #{job_id} is #{job.deadline} and now is #{current_timestamp}"
      # If the deadline is already passed, assign as many as workers for the job.
      if current_timestamp > job.deadline
        @logger.warn "Deadline of #{job_id} is #{job.deadline} and now is #{current_timestamp}"
        @logger.warn "#{job_id} missed deadline; give as much as possble"
        return 0, [worker_by_throughput.size, job.progress.undone.size].min
      end

      # Compute the range of worker required to make the job meet its deadline
      needed_worker = 0
      total_throughput = 0.0
      required_throughput = job.progress.undone.size / Float(job.deadline-current_timestamp)
      worker_by_throughput.each_with_index do |worker_id|
        break if total_throughput > required_throughput
        break if needed_worker == job.progress.undone.size
        needed_worker += 1

        # Try to estimate run time then get workers to occupy
        if job.task_running_time_on_worker[worker_id] != nil
          estimated_running_time = job.task_running_time_on_worker[worker_id]
          @logger.debug "Estimate run time using provided runtime = #{estimated_running_time} sec."
        elsif job.avg_task_running_time != nil && job.avg_task_running_time > 0
          @logger.debug "Estimate run time using average task execution time = #{job.avg_task_running_time} sec."
          estimated_running_time = job.avg_task_running_time
        else
          all = worker_avg_running_time.values.flatten
          avg = all / all.size rescue nil
          if avg != nil && avg > 0
            estimated_running_time = avg
            @logger.debug "Estimate run time using system average = #{estimated_running_time} sec."
          else
            estimated_running_time = INTIAL_EXEC_TIME_GUESS
            @logger.debug "Estimate run time using default value = #{estimated_running_time} sec."
          end
        end
        total_throughput += 1.0 / estimated_running_time
      end
      return 0, needed_worker
    end

    private :get_required_worker_range
  end
end
