require 'thread'

require_relative 'decision_maker'
class Dispatcher
  attr_accessor :status_checker, :decision_maker

  def initialize()
    # TODO: read-write locks for the job_worker_table
    @job_worker_table = {}
  end

  # APIs
  def get_worker(job_id)
    return @job_worker_table[job_id].pop()
  end

  def submit_jobs(job_list)
    # TODO: accept jobs
    # TODO: Generate and return UUID for jobs
  end

  def job_done(uuid)
    # TODO: Remove entries and queues
    # TODO: Reschedule jobs
  end

  def on_worker_available(worker)
    # TODO: Push into corresponding queue
  end

  def reschedule_jobs()
    decision_maker.schedule_jobs
  end

end
