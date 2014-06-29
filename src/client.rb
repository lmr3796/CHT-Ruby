require 'drb'
require 'logger/colors'
require 'thread'
require 'time'
require 'sync'

require_relative 'job'
require_relative 'common/read_write_lock_hash'
require_relative 'common/thread_pool'


class MessageService
  def << (m)
    @msg_queue << m
  end
  def initialize(uuid, dispatcher, logger)
    @msg_queue = Queue.new
    @uuid = uuid
    @dispatcher = dispatcher
    @logger=logger

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
      begin
        msg = @dispatcher.get_message @uuid
        @logger.debug "Received #{msg.inspect}" unless msg == nil || msg.empty?
        msg.each {|m| @msg_queue << m}
      rescue Timeout::Error
        retry
      end
    end
  end
  def process_message_queue
    loop do
      msg = @msg_queue.pop
      msg.each do|m|
        #TODO implement handler
      end
    end
  end
end
class Client
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
    @msg_service = MessageService.new(@uuid, @dispatcher, @logger)
    @msg_service.start

    #TODO: register on worker available handler

    #job_id_list = send_jobs(@jobs)
    ## TODO a better way to send out result
    #@result = Array.new(job_id_list.size){Hash.new}
    #@thread_id_list = job_id_list.each_with_index.map{ |job_id,i|
    #  @result[i][:deadline] = @jobs[i].deadline
    #  @thread_pool.schedule{
    #    run_job(job_id, i)
    #  }
    #}
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

  def run_task_on_worker(task, job_id, worker)
    # TODO: Task execution failure???
    @logger.debug "#{job_id} popped a task to worker #{worker}"
    begin
      worker_server = DRbObject.new_with_uri @dispatcher.worker_uri worker
      res = worker_server.run_task(task, job_id)
      @logger.debug "#{job_id} received result from worker #{worker} in #{res.run_time} seconds"
      worker_server.log_running_time job_id, res.run_time
      worker_server.release
      @logger.debug "#{job_id} released worker #{worker}"
      @dispatcher.one_task_done(job_id)
    rescue Exception => e
      @logger.error "#{job_id} exception raised by worker #{worker}: \"#{e.message}\", add task back to queue"
      @submitted_jobs[job_id].push(task)
    end
  end
  private :run_task_on_worker

end
