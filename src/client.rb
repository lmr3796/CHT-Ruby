require 'drb'
require 'logger/colors'
require 'thread'
require 'time'
require 'sync'

require_relative 'dispatcher'
require_relative 'job'
require_relative 'message_service'
require_relative 'common/rwlock_hash'

class ResultLostError; end

# TODO: Reimplement this with mixin polymorphism!
module ClientMessageHandler include MessageService::Client::MessageHandler
  # Implement handlers here, message {:type => [type]...} will use kernel#send
  # to dynamically invoke `MessageHandler#on_[type]`, passing the message as
  # the only parameter.

  def on_worker_available(m)
    m.is_a? MessageService::Message or raise ArgumentError
    @logger.info "Worker #{m.content[:worker]} assigned for job #{m.content[:job_id]}"
    job_id = m.content[:job_id]
    worker = m.content[:worker]
    worker_server = DRbObject.new_with_uri(@dispatcher.worker_uri(worker))
    task = @task_queue[job_id].pop(true) # Nonblocked, raise error if empty

    if worker_server.submit_task(task, @uuid)
      @dispatcher.task_sent(job_id)
      @logger.debug "Popped #{job_id}[#{task.id}] to worker #{worker}"
    else
      # On submission rejected
      @logger.warn "Submission of #{job_id}[#{task.id}] rejected by #{worker}. Worker probably gets scheduled to another job"
      redo_task(task.id, job_id)
    end
  rescue ThreadError # On empty task Queue
    # Workers may finish and come back
    # before we fetch last result and delete job.

    @logger.warn "#{job_id} received worker #{worker} but no task to process"
    @dispatcher.reschedule
    sleep 1 # Keep it from loop arrviing, debug use
  rescue DRb::DRbConnError
    @logger.error "Error contacting worker #{worker}"
    #TODO some recovery??
    return
  rescue => e
    @logger.error e.message
    @logger.error e.backtrace.join("\n")
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
    to_delete = {}
    @results.each do |j_id, res_list|
      obtained_tasks = res_list.each_index.select{|i|res_list[i] != nil}
      to_delete[j_id] = obtained_tasks unless obtained_tasks.empty?
    end
    clear_request = Worker::ClearResultRequest.new(@uuid, to_delete)
    worker_server.clear_result(clear_request)
    @logger.info "Deleted obtained results of #{job_id} on worker #{worker}"


    job_done(job_id) if !@results[job_id].include? nil

    # Notified to retrieve but not found, mark as lost
    raise ResultLostError if @results[job_id][task_id] == nil
    return
  rescue ResultLostError
    # FIXME try to handle this
    raise NotImplementedError
    logger.error "Result of #{job_id}[#{task_id}] missing, ask to redo"
    redo_task(task_id, job_id)
  rescue => e
    @logger.error e.message
    @logger.error e.backtrace.join("\n")
  end

end

class Client
  include ClientMessageHandler
  attr_reader :uuid, :results, :finish_time, :submitted_jobs

  def initialize(dispatcher_uri, jobs=[], logger=Logger.new(STDERR))
    DRb.start_service
    @rwlock = ReadWriteLock.new
    @submitted_jobs = ReadWriteLockHash.new
    @task_queue = ReadWriteLockHash.new
    @job_done = ReadWriteLockHash.new
    @finish_time = {}
    @dispatcher = DRbObject.new_with_uri(dispatcher_uri)
    @jobs = jobs
    @results = {}
    @logger = logger
    return
  end

  def stop()
    @msg_service.stop
    @dispatcher.unregister_client(self)
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
    test_msg = MessageService::Message.new(:chat, nil, "Test!, I'm #{@uuid}")
    @dispatcher.push_message(@uuid, test_msg)
    @logger.info "Test message sent."
    submit_jobs(@jobs) unless @jobs.empty?
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

    job_id_list = @dispatcher.submit_jobs(jobs, @uuid)
    raise 'Submissiion failure' if job_id_list != jobs.keys
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

    @rwlock.with_write_lock do
      results.each do |r|
        raise "Invalid task_id for #{job_id}" if r.task_id < 0
        raise "Invalid task_id for #{job_id}" if @results[job_id].size <= r.task_id
        # TODO Conflict results might come before we delete it on worker.
        # We currently ignore this
        @results[job_id][r.task_id] = r
        @logger.info "Updated result of #{job_id}[#{r.task_id}]"
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

  def redo_task(task_id, job_id)
    @task_queue[job_id] << @submitted_jobs[job_id].task[task_id]
    @dispatcher.redo_task(job_id)
    return
  end
end
