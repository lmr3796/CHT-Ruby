require 'drb'

require_relative '../config/config'
require_relative 'job'

# TODO: make client job queue table another class and abstracts the mutex calls
class Client

  def initialize()
    DRb.start_service

    @dispatcher = DRb.new_with_uri CHT_Configuration::Address.get_uri(CHT_Configuration::Address::DISPATCHER)

    # Ruby hashes are not thread safe; it must be protected by a mutex
    # TODO: read-write lock instead
    @submitted_jobs_mutex = Mutex.new
    @submitted_jobs = {}
  end

  def send_job(job)
    # Convert to a job list if a single job passed
    job = [job] unless jobs.is_a? Array
    job.each {|x| raise 'Parameters should be a list of jobs or a single job' if !x.is_a(Job)}
    uuid_list = @dispatcher.submit_jobs(job)
    raise 'Submission failed' if !uuid_list or !uuid_list.is_a? Array

    # Build a task queue for each job, indexed with uuid returned from dispatcher
    @submitted_jobs_mutex.synchronize {
      uuid_list.each_with_index{|uuid, index|
        task_queue = Queue.new
        job[index].each {|x| task_queue.push x}
        @submitted_jobs[uuid] = task_queue
      }
    }
    return
  end

  # Given a job, pop a task to it
  def run_job_on_given_worker(uuid, worker)
    task = None
    @submitted_jobs_mutex.synchronize do
      task = @submitted_jobs[uuid].pop(true)
    end
    run_task_on_worker(task, uuid, worker) # TODO: run asynchronously
  rescue ThreadError
    # Nothing to pop in the queue
    return
  end

  def run_task_on_worker(task, uuid, worker)
    # TODO: Task execution failure???
    worker = DRb.new_with_uri CHT_Configuration::Address.get_uri(CHT_Configuration::Address::WORKER[worker])
    worker.run_task(task, uuid)
    @submitted_jobs_mutex.synchronize { @dispatcher.job_done(uuid) if @submitted_jobs[uuid].empty}
  end

end
