require 'drb'
require 'logger/colors'
require 'thread'
require 'time'
require 'timers'
require 'sync'

require_relative 'dispatcher'
require_relative 'job'
require_relative 'message_service'
require_relative 'common/rwlock_hash'

class ResultLostError < RuntimeError; end

# TODO: Reimplement this with mixin polymorphism!
module ClientMessageHandler include MessageService::Client::MessageHandler
  # Implement handlers here, message {:type => [type]...} will use kernel#send
  # to dynamically invoke `MessageHandler#on_[type]`, passing the message as
  # the only parameter.

  def on_task_preempted(m)
    m.is_a? MessageService::Message or raise ArgumentError
    job_id = m.content[:job_id]
    task_id = m.content[:task_id]
    worker_name = m.content[:worker]
    return if !@task_queue.has_key?(job_id) # Job outdated; nevermind it.
    @logger.warn "#{job_id}[#{task_id}] on #{worker_name} is preempted."
    @task_execution_checker.fire
  end

  def on_worker_available(m)
    m.is_a? MessageService::Message or raise ArgumentError
    @logger.info "Worker #{m.content[:worker]} assigned for job #{m.content[:job_id]}"
    job_id = m.content[:job_id]
    worker_name = m.content[:worker]
    worker_server = DRbObject.new_with_uri(m.content[:uri]) if m.content[:uri] != nil
    if !@task_queue.has_key?(job_id)
      @logger.warn("#{job_id} doesn't exist.")
      raise ThreadError
      return
    end
    task = @task_queue[job_id].pop(true)                              # Nonblocked, raise error if empty
    submit_task_to_worker(job_id, task, worker_name, worker_server)   # Handles DRb::DRbConnError inside
  rescue DRb::DRbConnError
    @logger.error "Error contacting worker #{worker_name}"
  rescue ThreadError # On no task to do
    # Workers may finish and come back before we fetch last result and delete job.
    @logger.warn "#{job_id} received worker #{worker_name} but no task to process"
    @task_execution_checker.fire
    @logger.warn "#{job_id} progress: #{@dispatcher.get_progress(job_id).inspect}"
    @logger.warn "#{job_id} assignment: #{@execution_assignment.select{|k,v|k[0] == job_id}}"
    @logger.warn "#{job_id} task queue size: #{@task_queue[job_id].size}"
    @logger.warn "#{job_id} result nil: #{@results[job_id].each_with_index.select{|r,i| r==nil}.map{|r,i| i}}"
    @dispatcher.reschedule
    assignment_valid = worker_server.validate_occupied_assignment
    worker_server.release(@uuid) if assignment_valid
  rescue => e
    @logger.error e.message
    @logger.error e.backtrace.join("\n")
  ensure
    return
  end

  def on_task_result_available(m)
    m.is_a? MessageService::Message or raise ArgumentError
    job_id = m.content[:job_id]
    task_id = m.content[:task_id]
    worker = m.content[:worker]
    @logger.debug "Result of #{job_id}[#{task_id}] is ready"
    # Outdated result message
    @logger.debug "Result of #{job_id}[#{task_id}] is already on hand" and return if @results[job_id][task_id] != nil

    # Might retrieve results other than those in the message
    worker_server = DRbObject.new_with_uri(@dispatcher.worker_uri(worker))
    @logger.info "Fetching result of #{job_id}[#{task_id}] from #{worker}"
    fetched_results = worker_server.get_results(@uuid, job_id)
    @logger.info "Fetched #{fetched_results.size} result from #{worker}"

    # Update the results
    add_results(fetched_results, job_id)

    # Clear results that are on hand...
    @logger.info "Deleting obtained results of #{job_id} on worker #{worker}"
    task_id_to_delete = @results[job_id].each_with_index.select{|r, i|r != nil}.map{|r,i| i}
    clear_request = Worker::ClearResultRequest.new(job_id, task_id_to_delete, @uuid)
    worker_server.clear_result(clear_request)
    @logger.info "Deleted obtained results of #{job_id} on worker #{worker}"


    job_done(job_id) if !@results[job_id].include? nil

    # Notified to retrieve but not found, mark as lost
    raise ResultLostError if @results[job_id][task_id] == nil # TODO: more detection
    return
  rescue ResultLostError
    on_result_lost
  rescue => e
    @logger.error e.message
    @logger.error e.backtrace.join("\n")
  end

end

class Client
  include ClientMessageHandler
  attr_reader :uuid, :results, :submit_time, :finish_time, :submitted_jobs, :rwlock
  RESULT_POLLING_INTERVAL = 10

  def submit_task_to_worker(job_id, task, worker_name, worker_server)
    raise ArgumentError if job_id == nil
    raise ArgumentError if task == nil
    raise ArgumentError if worker_server== nil
    submission_status = nil
    begin
      submission_status = worker_server.submit_task(task, @uuid)
    rescue DRb::DRbConnError
      @logger.error "Error contacting worker #{worker_name} to submit the task."
    end

    if !submission_status
      # On submission rejected or failed
      @logger.warn "Submission of #{job_id}[#{task.id}] rejected by #{worker_name}. Worker probably gets scheduled to another job"
      redo_task(job_id, task.id)
      return
    end
    @execution_assignment[[job_id, task.id]] = worker_server
    @logger.debug "Popped #{job_id}[#{task.id}] to worker #{worker_name}"
    begin
      @dispatcher.task_sent(job_id)
      @logger.debug "Notified dispacher for popping #{job_id}[#{task.id}] to worker #{worker_name}"
    rescue DRb::DRbConnError
      @logger.error "Error notifing dispacher for popping #{job_id}[#{task.id}] to worker #{worker_name}"
    end
    return
  end
  private :submit_task_to_worker

  def initialize(dispatcher_uri, logger=Logger.new(STDERR))
    @rwlock = ReadWriteLock.new
    @submitted_jobs = ReadWriteLockHash.new
    @task_queue = ReadWriteLockHash.new
    @job_done = ReadWriteLockHash.new
    @execution_assignment = ReadWriteLockHash.new # maintains worker uris
    @submit_time = {}
    @finish_time = {}
    @results = {}
    @dispatcher = DRbObject.new_with_uri(dispatcher_uri)
    @logger = logger
    run_periodic_task_execution_checker
    return
  end

  def stop()
    stop_periodic_task_execution_checker
    @msg_service.stop
    @dispatcher.unregister_client(@uuid)
    @logger.info "Unregistered from the system"
    return
  end

  def done?(job_id_list=nil)
    raise ArgumentError if !job_id_list.is_a? Array
    raise ArgumentError, "Invalid job id(s) provided" if !(job_id_list - @submitted_jobs.keys).empty?
    raise ArgumentError, "Invalid job id(s) provided" if !(job_id_list - @job_done.keys).empty?
    @logger.debug "Checking on #{job_id_list}"
    job_id_list.each{|j| return false unless @job_done[j]}
    return true
  rescue ArgumentError
    @logger.fatal job_id_list
    raise
  end

  def wait(job_id_list=nil)
    # Default value nil stands for all jobs
    # Should not use default value parameter for obtaining this must be done using read lock
    if job_id_list != nil
      raise ArgumentError if !job_id_list.is_a? Array
      raise ArgumentError, "Invalid job id(s) provided" if !(job_id_list - @submitted_jobs.keys).empty?
      raise ArgumentError, "Invalid job id(s) provided" if !(job_id_list - @job_done.keys).empty?
    end
    until @rwlock.with_read_lock{done?(job_id_list == nil ? @submitted_jobs.keys : job_id_list)} do
      @logger.debug "There are still jobs undone, keep waiting"
      Thread::stop
    end
    @logger.info "Jobs#{job_id_list} are #{"all " if job_id_list == nil}done!"
    return
  end

  def wait_all()
    wait
    return
  end

  def register
    @uuid = @dispatcher.register_client
    @logger.info "Registered client to the system, uuid=#{@uuid}"
    @msg_service = MessageService::Client.new(@uuid, @dispatcher, self)
    @msg_service.logger = @logger
    @logger.info "Initialized message service."
  end

  def start()
    @msg_service.start
    @logger.info "Running message service."
    @logger.info "Sending testing message."
    @dispatcher.push_message(@uuid, MessageService::TEST_MESSAGE)
    @logger.info "Test message sent."
    return
  end

  def submit_jobs(jobs)
    # Convert to a job list if a single job passed
    raise ArgumentError if jobs == nil
    jobs = [jobs] unless jobs.is_a? Array
    jobs.reject{|x|x.is_a? Job}.empty? or raise ArgumentError, 'Parameters should be a list of jobs or a single job'

    @logger.info "Submitting #{jobs.size} job(s)"

    # This is a 2-pass negotiation. The reason to have it is because the local
    # information might not be ready before a worker available message comes.
    @logger.info "Generating job uuids"
    job_id_list = @dispatcher.generate_job_id(jobs, @uuid)
    raise ArgumentError, "ID amount mismatch" if job_id_list.size != jobs.size
    @logger.info "job uuids: #{job_id_list}"

    @logger.info "Preparing local information of jobs: #{job_id_list}"
    jobs = Hash[job_id_list.zip(jobs)]
    job_id_list.each do |job_id|  # Build a task queue for each job, indexed with job_id returned from dispatcher
      @submitted_jobs[job_id] = jobs[job_id]
      @results[job_id] = [nil] * jobs[job_id].task.size
      @job_done[job_id] = false
      tq = Queue.new # must be synchronized for it's consumed under multithreaded env.
      jobs[job_id].task.each do |t|
        t.job_id = job_id
        tq << t
      end
      @task_queue[job_id] = tq
    end
    @rwlock.with_write_lock{@task_execution_checker.continue}   # Must make sure poller is prepared

    job_id_list = @dispatcher.submit_jobs(jobs, @uuid)
    raise 'Submissiion failure' if job_id_list != jobs.keys
    submit_time = Time.now
    job_id_list.each{|job_id|@submit_time[job_id] = submit_time}
    # TODO submission failure??
    @logger.info "Job submitted: id mapping: #{job_id_list}"
    return job_id_list
  end

  def add_results(results, job_id)
    results = [results] if !results.is_a?(Array)
    results.each do |r|
      r.is_a? TaskResult or raise ArgumentError, 'Invalid TaskResult(s)'
      r.job_id == job_id or raise ArgumentError, 'Job id mismatched'
    end

    # Log down results
    @rwlock.with_write_lock do
      results.each do |r|
        raise "Invalid task_id for #{job_id}" if r.task_id < 0
        raise "Invalid task_id for #{job_id}" if @results[job_id].size <= r.task_id
        # Conflict results might come before we delete it on worker.
        # We currently ignore this
        next if @results[job_id][r.task_id] != nil

        @results[job_id][r.task_id] = r
        @logger.info "Updated result of #{job_id}[#{r.task_id}]"

        # TODO what if poller find out before this???
        @execution_assignment.delete([job_id, r.task_id])
        @logger.debug "Updated running status of #{job_id}[#{r.task_id}]"
      end
    end

    return
  end

  def job_done(job_id)
    raise ArgumentError if !@submitted_jobs.has_key? job_id
    return if @rwlock.with_read_lock{@job_done[job_id]}
    finish_time = Time.now
    @rwlock.with_write_lock do
      @finish_time[job_id] = finish_time
      @job_done[job_id] = true
    end
    @logger.info "Job #{job_id} completed at #{@finish_time[job_id]}, ask to delete."
    delete_job(job_id)
    Thread::main.run and @logger.debug "Notifies main thread wait to check if waiting jobs are done"
  end

  def delete_job(job_id)
    @logger.info "Contact dispatcher to delete job #{job_id}"
    @dispatcher.delete_job(job_id, @uuid)
    @logger.info "Deleted job #{job_id}"
    return
  end

  def stop_periodic_task_execution_checker
    @task_execution_checker_timer_group.pause_all
    @timer_thr.kill
  end

  def run_periodic_task_execution_checker
    @task_execution_checker_timer_group = Timers::Group.new
    @task_execution_checker = @task_execution_checker_timer_group.every(RESULT_POLLING_INTERVAL) do
      missing_task = check_missing_task
      next if missing_task.empty?
      @dispatcher.tell_worker_down_detected
      missing_task.each{|job_id, task_id| on_result_lost(job_id, task_id)}
      # Stop polling if no pending jobs
      @rwlock.with_write_lock do
        if done?(@submitted_jobs.keys)
          @logger.warn "No pending jobs, no need to check for results"
          @task_execution_checker.pause
        end
      end
    end
    @timer_thr = Thread.new do
      loop do
        @task_execution_checker_timer_group.wait
      end
    end
  end
  private :run_periodic_task_execution_checker

  def check_missing_task
    missing = []
    ass = Hash.new.merge(@execution_assignment) # each is not implemented with rwlock...
    ass.each do |j_and_t, worker_server|
      # Workers may be failed here
      job_id, task_id = j_and_t
      begin
        missing << j_and_t if worker_server.current_execution_of(@uuid) != j_and_t &&
          !worker_server.exist_result?(job_id, task_id, @uuid)
      rescue DRb::DRbConnError => e
        @logger.error "Worker is found down, take it as lost."
        @logger.error e.message
        missing << j_and_t
      rescue => e
        @logger.error e.message
        @logger.error e.backtrace.join("\n")
      end
    end
    return missing
  end

  def on_result_lost(job_id, task_id)
    @logger.warn "Result of #{job_id}[#{task_id}] lost or preempted, asked to redo"
    redo_task(job_id, task_id)
    # In this case, we've already told dispatcher task sent, so tell it to redo is necessary
    @dispatcher.redo_task(job_id)
    return
  end

  def redo_task(job_id, task_id)
    @execution_assignment.delete([job_id, task_id])
    @task_queue[job_id] << @submitted_jobs[job_id].task[task_id]
    return
  end
end
