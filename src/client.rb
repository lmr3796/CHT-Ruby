require 'drb'
require 'logger/colors'
require 'thread'
require 'time'
require 'sync'

require_relative 'dispatcher'
require_relative 'job'
require_relative 'message_service'
require_relative 'common/rwlock_hash'


module ClientMessageHandler include MessageService::Client::MessageHandler
  # Implement handlers here, message {:type => [type]...} will use kernel#send
  # to dynamically invoke `MessageHandler#on_[type]`, passing the message as
  # the only parameter.

  def on_worker_available(m)
    @logger.debug "Worker #{m.content[:worker]} assigned for job #{m.content[:job_id]}" 
    job_id = m.content[:job_id]
    worker = m.content[:worker]
    task = @submitted_jobs[job_id][:task_queue].pop(true) # Nonblocked, raise error if empty
    worker_server = DRbObject.new_with_uri(@dispatcher.worker_uri(worker))
    worker_server.submit_task(task, job_id, @uuid)
    @dispatcher.task_sent(job_id)
    @logger.debug "#{job_id} popped a task to worker #{worker}"
  rescue ThreadError # On empty task Queue
    @logger.warn "#{job_id} received worker #{worker} but no task to process"
    #TODO some notification to dispatcher????
  rescue DRb::DRbConnError
    @logger.error "Error contacting worker #{worker}"
    #TODO some recovery??
    return
  end

  def on_task_result_available(m)
    return if @results[m[:job_id]][m[:task_id]] != nil  # Outdated result message
    job_id = m[:job_id]
    task_id = m[:task_id]
    worker_server = DRbObject.new_with_uri(@dispatcher.worker_uri(m[:worker]))
    results = worker_server.get_results(@uuid)
    add_results(results, job_id)
    redo_task(task_id, job_id) if @results[job_id][task_id] == nil
    return
  end
end

class Client
  include ClientMessageHandler
  attr_reader :uuid, :results
  DEFAULT_THREAD_POOL_SIZE = 32

  def initialize(dispatcher_uri, jobs=[], logger=Logger.new(STDERR))
    DRb.start_service
    @submitted_jobs = ReadWriteLockHash.new
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

  def wait_all()
    loop{}
    return
  end

  def start()
    @rwlock = ReadWriteLock.new
    @uuid = @dispatcher.register_client
    @logger.info "Registered client to the system, uuid=#{@uuid}"
    @msg_service = MessageService::Client.new(@uuid, @dispatcher, self)
    @msg_service.logger = @logger
    @logger.info "Initialized message service."
    @msg_service.start
    @logger.info "Running message service."
    @logger.info "Sending testing message."
    test_msg = MessageService::Message.new(:chat, nil, "Test!, I'm #{@uuid}")
    @dispatcher.push_message(@uuid, test_msg)
    submit_jobs(@jobs) unless @jobs.empty?
    return
  end

  def submit_jobs(jobs)
    # Convert to a job list if a single job passed
    raise ArgumentError if jobs == nil
    jobs = [jobs] unless jobs.is_a? Array
    jobs.reject{|x|x.is_a? Job}.empty? or raise ArgumentError, 'Parameters should be a list of jobs or a single job'

    @logger.info "Submitting #{jobs.size} job(s)"
    job_id_list = @dispatcher.submit_jobs(jobs, @uuid)
    (@logger.error "Submission failed"; raise 'Submission failed') if !job_id_list or !job_id_list.is_a? Array
    @logger.info "Job submitted: id mapping: #{job_id_list}"
    # Build a task queue for each job, indexed with job_id returned from dispatcher
    job_id_list.each_with_index do |job_id, i|
      @submitted_jobs[job_id] = {
        :task_queue => Queue.new, # must be synchronized for it's consumed under multithreaded env.
        :job => jobs[i]
      }
      jobs[i].task.each{|t| @submitted_jobs[job_id][:task_queue] << t}
      @results[job_id] = [nil] * jobs[i].task.size
    end
    @logger.info "Update local status of #{job_id_list}"
    return job_id_list
  end

  def add_results(results, job_id)
    raise ArgumentError if results == nil
    raise ArgumentError if job_id == nil
    results = [r] unless r.is_a? Array
    results.each do |r|
      r == nil || r.is_a?(TaskResult) or raise ArgumentError, 'Invalid TaskResult(s)'
      r.job_id == job_id or raise ArgumentError, 'Job id mismatched'
    end
    @rwlock.with_write_lock do
      results.each do |r|
        raise "Invalid task_id for #{job_id}" if @results[job_id].size <= r.task_id || r.task_id < 0
        raise 'Conflicted result' if @results[job_id][r.task_id] != nil
        @results[job_id][r.task_id] = r
      end
    end # assign only on no result
    return
  end

  def redo_task(task_id, job_id)
    @submitted_job[job_id][:task_queue] << @submitted_job[job_id][:job].task[task_id]
    @dispatcher.redo_task(job_id)
    return
  end
end
