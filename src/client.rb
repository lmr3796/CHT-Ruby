require 'drb'
require 'logger/colors'
require 'thread'
require 'time'
require 'sync'

require_relative 'job'
require_relative 'common/read_write_lock_hash'
require_relative 'common/thread_pool'


class Client
  attr_accessor :jobs, :result
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

  def register
    @uuid = @dispatcher.register_client
    @logger.info "Registered client to the system, uuid=#{@id}"
    @notification_thr = Thread.new do
      Thread.stop # Don't run immediately, wait for client to start
      loop do
        begin
          msg = @dispatcher.get_message self
          @logger.debug "Received #{msg.inspect}" unless msg == nil
        rescue Timeout::Error
          retry
        end
      end
    end
  end

  def stop()
    @dispatcher.unregister_client(self)
    @logger.info "Unregistered from the system"
  end

  def start(blocking=false)
    # DEBUG!!!
    register
    # TODO: send_jobs
    @logger.info "Running notification service."
    @notification_thr.run
    return
    # DEBUG!!!
    job_id_list = send_jobs(@jobs)
    # TODO a better way to send out result
    @result = Array.new(job_id_list.size){Hash.new}
    @thread_id_list = job_id_list.each_with_index.map{ |job_id,i|
      @result[i][:deadline] = @jobs[i].deadline
      @thread_pool.schedule{
        run_job(job_id, i)
      }
    }
    return @thread_id_list unless blocking
    wait_all
  end

  def wait_all()
    @thread_id_list.each{|thread_id| wait(thread_id)}
  end

  def wait(thread_id)
    @thread_pool.join(thread_id)
  end

  def run_job(job_id, result_pos)
    @result[result_pos][:deadline] = Time.now
    task_queue = @submitted_jobs[job_id]
    thread_id_list = []
    until task_queue.empty? do
      until task_queue.empty? do
        task = task_queue.pop
        @logger.debug "#{job_id} waiting for worker"
        worker = @dispatcher.require_worker(job_id)
        @logger.debug "#{job_id} assigned with worker #{worker}"
        thread_id_list << @thread_pool.schedule {
          #TODO: Task execution failure???
          run_task_on_worker(task, job_id, worker)
        }
      end
      thread_id_list.each{ |thread_id|
        @thread_pool.join(thread_id)
      }
    end
    @logger.info "Job #{job_id} is done"
    @result[result_pos][:finish_time] = Time.now
    @dispatcher.job_done(job_id)
  end
  private :run_job

  def send_jobs(jobs)
    # Convert to a job list if a single job passed
    jobs = [jobs] unless jobs.is_a? Array
    jobs.each {|x| raise 'Parameters should be a list of jobs or a single job' if !x.is_a? Job}
    @logger.info "Submitting #{jobs.size} job(s)"
    job_id_list = @dispatcher.submit_jobs(jobs)
    (@logger.error "Submission failed"; raise 'Submission failed') if !job_id_list or !job_id_list.is_a? Array
    @logger.info "Job submitted: id mapping: #{job_id_list}"
    # Build a task queue for each job, indexed with job_id returned from dispatcher
    job_id_list.each_with_index{|job_id, index|
      task_queue = Queue.new
      tasks = jobs[index].task
      tasks.each {|x| task_queue.push x}
      @submitted_jobs[job_id] = task_queue
    }
    return job_id_list
  end
  private :send_jobs

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
