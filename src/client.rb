require 'drb'
require 'logger/colors'
require 'thread'
require 'time'
require 'sync'

require_relative 'dispatcher'
require_relative 'job'
require_relative 'message_service'
require_relative 'common/read_write_lock_hash'
require_relative 'common/thread_pool'


module ClientMessageHandler include MessageService::Client::MessageHandler
  # Implement handlers here, message {:type => [type]...} will use kernel#send
  # to dynamically invoke `MessageHandler#on_[type]`, passing the message as
  # the only parameter.
  def on_invalid_message_error(msg, err)
    @logger.warn("Error on parsing message, msg=#{m.inspect}")
    @logger.warn e.message
    @logger.warn e.backtrace.join("\n")
  end

  def on_no_handler_found_error(msg, err)
    @logger.warn("No handler #{handler_name} for #{msg[:type]} found, msg=#{msg.inspect}")
  end

  def on_worker_available(m)
    task = @submitted_job[m[:job_id]][:task_queue].pop(true) # Nonblocked, raise error if empty
    worker = m[:worker]
    worker_server = DRbObject.new_with_uri(@dispatcher.worker_uri(worker))
    worker_server.submit_task(task, job_id, @uuid)
    @dispatcher.task_sent(job_id)
    @logger.debug "#{job_id} popped a task to worker #{worker}"
  rescue ThreadError # On empty task Queue
    @logger.warn "#{job_id} received worker #{worker} but no task to process"
    #TODO some notification to dispatcher????
  end

  def on_task_result_available(m)
    return if @results[m[:job_id]][m[:task_id]] != nil  # Outdated result message
    job_id = m[:job_id]
    task_id = m[:task_id]
    worker_server = DRbObject.new_with_uri @dispatcher.worker_uri m[:worker]
    results = worker_server.get_results(@uuid)
    add_results(results, job_id)
    redo_task(task_id, job_id) if @results[job_id][task_id] == nil
  end
end

class Client
  include ClientMessageHandler
  attr_accessor :jobs, :logger
  attr_reader :uuid, :results
  DEFAULT_THREAD_POOL_SIZE = 32

  def initialize(dispatcher_uri, jobs=[], thread_pool_size=DEFAULT_THREAD_POOL_SIZE, logger=Logger.new(STDERR))
    DRb.start_service
    @submitted_jobs = ReadWriteLockHash.new
    @thread_pool = ThreadPool.new(thread_pool_size)
    @dispatcher = DRbObject.new_with_uri(dispatcher_uri)
    @jobs = jobs
    @results = {}
    @logger = logger
  end

  def stop()
    @msg_service.stop
    @dispatcher.unregister_client(self)
    @logger.info "Unregistered from the system"
  end

  def wait_all()
    @thread_id_list.each{|thread_id| wait(thread_id)}
  end

  def start(blocking=false)
    @rwlock = ReadWriteLock.new
    @uuid = @dispatcher.register_client
    @logger.info "Registered client to the system, uuid=#{@uuid}"
    @msg_service = MessageService::Client.new(@uuid, @dispatcher, self)
    @logger.info "Initialized message service."
    @msg_service.start
    @logger.info "Running message service."
    wait_all if blocking
    return
  end

  def send_jobs(jobs)
    # Convert to a job list if a single job passed
    raise ArgumentError if jobs == nil
    jobs = [jobs] unless jobs.is_a? Array
    jobs.select{!x.is_a? Job}.empty? or raise ArgumentError, 'Parameters should be a list of jobs or a single job'

    @logger.info "Submitting #{jobs.size} job(s)"
    job_id_list = @dispatcher.submit_jobs(jobs)
    (@logger.error "Submission failed"; raise 'Submission failed') if !job_id_list or !job_id_list.is_a? Array
    @logger.info "Job submitted: id mapping: #{job_id_list}"
    # Build a task queue for each job, indexed with job_id returned from dispatcher
    job_id_list.each_with_index do |job_id, i|
      @submitted_jobs[job_id] = {
        :task_queue => Queue.new, # must be synchronized for it's consumed under multithreaded env.
        :job => jobs[i]
      }
      j.task.each{|t| @submitted_jobs[job_id][:task_queue] << t}
      @results[job_id] = [nil] * jobs[i].task.size
    end
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
  end

  def redo_task(task_id, job_id)
    @submitted_job[job_id][:task_queue] << @submitted_job[job_id][:job].task[task_id]
    @dispatcher.redo_task(job_id)
  end
end
