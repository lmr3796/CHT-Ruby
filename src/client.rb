require 'drb'
require 'logger/colors'
require 'thread'
require 'time'
require 'sync'

require_relative 'dispatcher'
require_relative 'job'
require_relative 'common/read_write_lock_hash'
require_relative 'common/thread_pool'


module MessageHandler
  # Implement handlers here, message {:type => [type]...} will use kernel#send
  # to dynamically invoke `MessageHandler#on_[type]`, passing the message as
  # the only parameter.
  def on_chat(m)  # For testing :P
    @logger.debug "on_chat: Received \"#{m[:str]}\""
  end

  def on_worker_available(m)
    task = @submitted_job[m[:job_id]][:task_queue].pop(true) # Nonblocked, raise error if empty
    worker_server = DRbObject.new_with_uri @dispatcher.worker_uri worker
    @logger.debug "#{job_id} popped a task to worker #{worker}"
    worker_server.submit_task(task, job_id, @uuid)
  rescue ThreadError # On empty task Queue
    #TODO some notification to dispatcher????
  end
end

class MessageService
  def initialize(uuid, dispatcher, handler)
    uuid.is_a? String or raise ArgumentError
    handler.is_a? MessageHandler or raise ArgumentError
    @msg_queue = Queue.new
    @uuid = uuid
    @dispatcher = dispatcher
    @handler = handler
    @logger = @handler.logger

    # Producer && consumer
    @notification_thr = Thread.new do
      Thread.stop # Don't run immediately, wait for client to start
      poll_message
    end
    @process_thr = Thread.new do
      Thread.stop # Don't run immediately, wait for client to start
      process_message_queue
    end
    @logger.info "Initialized message service; uuid=#{@uuid}"
  end

  def << (m)
    @msg_queue << m
  end

  def start
    @logger.info "Running message service; uuid=#{@uuid}"
    @notification_thr.run
    @process_thr.run
  end

  def stop
    @notification_thr.kill
    @process_thr.kill
  end

  def poll_message
    loop do
      # Timeout must be implemented on server side since drb won't release wait on error...
      msg = @dispatcher.get_message @uuid
      next if msg.empty?
      msg.each {|m| @msg_queue << m}
    end
  end

  def process_message_queue
    loop do
      m = @msg_queue.pop
      begin
        handler_name = "on_#{m[:type].to_s}"
        @handler.respond_to?(handler_name) ?
          @handler.send(handler_name, m) :  # The ruby way to invoke method by its name string
          @logger.warn("No handler #{handler_name} for #{m[:type]} found, msg=#{m.inspect}")
      rescue => e
        @logger.warn("Error on parsing message, msg=#{m.inspect}")
        @logger.warn e.message
        @logger.warn e.backtrace.join("\n")
      end
    end
  end
end

class Client
  include MessageHandler
  attr_accessor :jobs, :result, :logger
  attr_reader :uuid
  DEFAULT_THREAD_POOL_SIZE = 32

  def initialize(dispatcher_uri, jobs=[], thread_pool_size=DEFAULT_THREAD_POOL_SIZE, logger=Logger.new(STDERR))
    DRb.start_service
    @submitted_jobs = ReadWriteLockHash.new
    @thread_pool = ThreadPool.new(thread_pool_size)
    @dispatcher = DRbObject.new_with_uri dispatcher_uri
    @jobs = jobs
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
    @uuid = @dispatcher.register_client
    @logger.info "Registered client to the system, uuid=#{@uuid}"
    @msg_service = MessageService.new(@uuid, @dispatcher, self)
    @msg_service.start

    #TODO: register on worker available handler
    return @thread_id_list unless blocking
    wait_all
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
      task_queue = Queue.new  # task_queue of a job must be synchronized for it's consumed under multithreaded env.
      jobs[i].task.each {|t| task_queue << t}
      @submitted_jobs[job_id] = task_queue
    end
    return job_id_list
  end

  #def run_task_on_worker(task, job_id, worker)
  #  # TODO: Task execution failure???
  #  @logger.debug "#{job_id} popped a task to worker #{worker}"
  #  begin
  #    worker_server = DRbObject.new_with_uri @dispatcher.worker_uri worker
  #    res = worker_server.run_task(task, job_id)
  #    @logger.debug "#{job_id} received result from worker #{worker} in #{res.run_time} seconds"
  #    worker_server.log_running_time job_id, res.run_time
  #    worker_server.release
  #    @logger.debug "#{job_id} released worker #{worker}"
  #    @dispatcher.one_task_done(job_id)
  #  rescue Exception => e
  #    @logger.error "#{job_id} exception raised by worker #{worker}: \"#{e.message}\", add task back to queue"
  #    @submitted_jobs[job_id].push(task)
  #  end
  #end
  private :run_task_on_worker

end
