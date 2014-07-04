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
    worker_server = DRbObject.new_with_uri(@dispatcher.worker_uri(worker))
    task = @submitted_jobs[job_id][:task_queue].pop(true) # Nonblocked, raise error if empty

    worker_server.submit_task(task, @uuid)
    @dispatcher.task_sent(job_id)
    @logger.debug "#{job_id} popped a task to worker #{worker}"
  rescue ThreadError # On empty task Queue
    #FIXME this shouldn't happen
    @logger.warn "#{job_id} received worker #{worker} but no task to process"
    worker_server.release(@uuid)  # It takes client id for authentication
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
    @rwlock = ReadWriteLock.new
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

    jobs = Hash[job_id_list.zip(jobs)]
    job_id_list.each do |job_id|  # Build a task queue for each job, indexed with job_id returned from dispatcher
      @submitted_jobs[job_id] = {
        :task_queue => Queue.new, # must be synchronized for it's consumed under multithreaded env.
        :job => jobs[job_id]
      }
      jobs[job_id].task.each do |t|
        t.job_id = job_id
        @submitted_jobs[job_id][:task_queue] << t
      end
      @results[job_id] = [nil] * jobs[job_id].task.size
    end

    job_id_list = @dispatcher.submit_jobs(jobs, @uuid)
    raise 'Submissiion failure' if job_id_list != jobs.keys
    # TODO submission failure??
    @logger.info "Job submitted: id mapping: #{job_id_list}"
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
