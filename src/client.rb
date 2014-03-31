require 'drb'
require 'logger'
require 'thread'
require 'sync'

require_relative 'job'
require_relative 'common/read_write_lock_hash'
require_relative 'common/thread_pool'


class Client
  attr_accessor :jobs
  DEFAULT_THREAD_POOL_SIZE = 32

  def initialize(dispatcher_uri, jobs=[], thread_pool_size=DEFAULT_THREAD_POOL_SIZE, logger=Logger.new(STDERR))
    DRb.start_service
    @submitted_jobs = ReadWriteLockHash.new
    @thread_pool = ThreadPool.new(thread_pool_size)
    @dispatcher = DRbObject.new_with_uri dispatcher_uri
    @jobs = jobs
  end

  def start(blocking=false)
    job_id_list = send_jobs(@jobs)
    @thread_id_list = job_id_list.map{ |job_id|
      @thread_pool.schedule{
        run_job(job_id)
      }
    }
    return @thread_id_list unless blocking
    wait_all
  end

  def wait_all()
    @thread_id_list.each{ |thread_id| wait(thread_id)}
  end

  def wait(thread_id)
    @thread_pool.join(thread_id)
  end

  def run_job(job_id)
    task_queue = @submitted_jobs[job_id]
    thread_id_list = []
    until task_queue.empty? do
      task = task_queue.pop
      worker = @dispatcher.require_worker(job_id)
      thread_id_list << @thread_pool.schedule{
        #TODO: Task execution failure???
        run_task_on_worker(task, job_id, worker)
      }
    end
    thread_id_list.each{ |thread_id|
      @thread_pool.join(thread_id)
    }
    @dispatcher.job_done(job_id)
  end
  private :run_job

  def send_jobs(jobs)
    # Convert to a job list if a single job passed
    jobs = [jobs] unless jobs.is_a? Array
    jobs.each {|x| raise 'Parameters should be a list of jobs or a single job' if !x.is_a? Job}
    job_id_list = @dispatcher.submit_jobs(jobs)
    raise 'Submission failed' if !job_id_list or !job_id_list.is_a? Array

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
    worker = DRb.new_with_uri CHT_Configuration::Address.get_uri(CHT_Configuration::Address::WORKER[worker])
    worker.run_task(task, job_id)
  end
  private :run_task_on_worker

end
