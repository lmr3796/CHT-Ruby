require 'drb'

require_relative '../config/config'
require_relative 'job'

class Client

  def initialize()
    dispatcher_uri = "druby://#{CHT::Config::DISPATCHER[:address]}:#{CHT::Config::DISPATCHER[:port]}"
    @dispatcher = DRb.new_with_uri dispatcher_uri
    DRb.start_service

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

end
