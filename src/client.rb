require 'drb'
require 'thread'
require 'sync'

require_relative '../config/config'
require_relative 'job'
require_relative 'common/read_write_lock_hash'
require_relative 'common/thread_pool'


class Client

  THREAD_POOL_SIZE = 32

  def initialize()
    DRb.start_service
    @dispatcher = DRbObject.new_with_uri CHT_Configuration::Address.druby_uri(CHT_Configuration::Address::DISPATCHER)

    @submitted_jobs = ReadWriteLockHash.new
    @thread_pool = ThreadPool.new(THREAD_POOL_SIZE)
  end

  def start(jobs)
    uuid_list = send_jobs(jobs)
    #For each job, create a thread in thread pool to run the tasks
    uuid_list.each{ |uuid|
      @thread_pool.schedule{
        run_job(uuid)
      }
    }
  end

  def run_job(uuid)
    task_queue = @submitted_jobs[uuid]
    until task_queue.empty? do
      task = task_queue.pop
      worker = @dispatcher.require_worker(uuid)
      @thread_pool.schedule{
        run_task_on_worker(task, uuid, worker)
      }
    end
    #TODO: Task execution failure???
    @dispatcher.job_done(uuid)
  end

  def send_jobs(jobs)
    # Convert to a job list if a single job passed
    jobs = [jobs] unless jobs.is_a? Array
    jobs.each {|x| raise 'Parameters should be a list of jobs or a single job' if !x.is_a(Job)}
    uuid_list = @dispatcher.submit_jobs(jobs)
    raise 'Submission failed' if !uuid_list or !uuid_list.is_a? Array

    # Build a task queue for each job, indexed with uuid returned from dispatcher
    uuid_list.each_with_index{|uuid, index|
      task_queue = Queue.new
      jobs[index].each {|x| task_queue.push x}
      @task_count[uuid] = jobs[index].size
      @submitted_jobs[uuid] = task_queue
    }
    return uuid_list
  end

  def run_task_on_worker(task, uuid, worker)
    # TODO: Task execution failure???
    worker = DRb.new_with_uri CHT_Configuration::Address.get_uri(CHT_Configuration::Address::WORKER[worker])
    worker.run_task(task, uuid)
  end

  private :run_job, :send_jobs, :run_task_on_worker
end
