require 'drb'
require 'publisher'
require 'thread'
require 'timeout'
require 'securerandom'

require_relative 'common/rwlock_hash'

require_relative 'job'
require_relative 'message_service'

require_relative 'base_server'
require_relative 'decision_maker'

class Dispatcher < BaseServer
  module DispatcherJobListChangeCallBack; end
  module DispatcherClientInterface; end
  module DispatcherWorkerInterface; end
  module MessageServiceServerDelegator; end
  include DispatcherJobListChangeCallBack
  include DispatcherClientInterface
  include DispatcherWorkerInterface
  include MessageServiceServerDelegator
end

#FIXME Publisher is synchronous, maybe have to change it into an async one?

class Dispatcher::JobList < ReadWriteLockHash  # {job_id => job_instance}
  extend Publisher
  can_fire :submission, :deletion
  def initialize(logger)
    super()
    @logger = logger
    return
  end

  def []=(job_id, job)
    r = super
    fire(:submission, [job_id])
    return r
  end

  def delete(job_id)
    super  # The job_id must be passed if super is not the first statement....
    @logger.info "Job #{job_id} deleted"
    fire(:deletion, [job_id])
    return
  end

  def merge!(jobs)
    super
    fire(:submission, jobs.keys)
    return self
  end
end

class Dispatcher::ClientJobList < ReadWriteLockHash
  def get_client_by_job(job_id)
    hash_clone.each{|c, jl| return c if jl.include?(job_id)}
    return
  end
end

class Dispatcher::ScheduleManager
  attr_reader :job_worker_table, :worker_job_table
  attr_writer :status_checker, :decision_maker

  def initialize(job_list, arg={})
    @logger = arg[:logger]
    @lock = ReadWriteLock.new
    @worker_job_table = {}
    @job_worker_table = {}
    @job_list = job_list
    @decision_maker = arg[:decision_maker]
    @status_checker = arg[:status_checker]
    return
  end

  def schedule_job()
    @logger.info 'Updating schedule'
    @lock.with_write_lock do
      begin
        job_running_time = @status_checker.job_running_time
        worker_avg_running_time = @status_checker.worker_avg_running_time
        cloned_job_list = @job_list.hash_clone
        workers_alive = @status_checker.worker_status.reject{|w,s| s == Worker::STATUS::DOWN || s == Worker::STATUS::UNKNOWN}
      rescue DRb::DRbConnError
        @logger.error "Error reaching status checker when scheduling"
        raise
      end

      begin
        # Schedule result:  {job_id => [worker1, worker2...]}
        @job_worker_table = @decision_maker.schedule_job(
          cloned_job_list,
          workers_alive, # Don't schedule on downed workers
          :job_running_time=>job_running_time,
          :worker_avg_running_time => worker_avg_running_time
        )
      rescue DRb::DRbConnError
        @logger.error "Error reaching decision maker when scheduling"
        raise
      end

      new_worker_job_table = Hash.new
      @job_worker_table.keys.each do |job_id|
        workers = @job_worker_table[job_id]
        workers.each{|worker| new_worker_job_table[worker] = job_id}
      end
      @worker_job_table.replace(new_worker_job_table)
    end
    @logger.info 'Updated schedule successfully'
    @logger.debug "Current schedule: #{@job_worker_table}"
  ensure
    return
  end

end

class Dispatcher < BaseServer
  attr_writer :status_checker, :decision_maker

  def initialize(arg={})
    super arg[:logger]
    @resource_mutex = Mutex.new
    #@job_worker_queues = ReadWriteLockHash.new
    @status_checker = arg[:status_checker]
    @decision_maker = arg[:decision_maker]
    @msg_service_server = MessageService::BasicServer.new

    @client_job_list = ClientJobList.new
    @job_list = JobList.new @logger
    @schedule_manager = ScheduleManager.new(@job_list,
                                            :status_checker => @status_checker,
                                            :decision_maker => @decision_maker,
                                            :logger => @logger)
    @job_list.subscribe(:submission, self, :on_job_submitted)
    @job_list.subscribe(:deletion, self, :on_job_deleted)
    return
  end

  def assign_worker_to_job(worker, job_id)
    @logger.debug "Worker #{worker} will be assigned to #{job_id}"
    job_assignment = [job_id, @client_job_list.get_client_by_job(job_id)]
    job_assignment = Worker::JobAssignment.new(job_id, @client_job_list.get_client_by_job(job_id))
    DRbObject.new_with_uri(worker_uri(worker)).assignment = job_assignment # May have to clean this ??
    return
  end
  private :assign_worker_to_job

  # General APIs

  # A delegator
  def reschedule()
    @schedule_manager.schedule_job
    return
  end

  def worker_uri(worker)
    return @status_checker.worker_uri(worker)
  end

  def log_job_worker_queue
    queue_status = @job_worker_queues.map{|j,wq| [j, "#{wq.size} wrks, #{wq.num_waiting} waiting"]}
    @logger.warn "Current queue status: #{queue_status}"
    return
  end

  def has_job?(job_id)
    raise ArgumentError if job_id == nil
    return @job_list.has_key? job_id
  end

end

module Dispatcher::DispatcherJobListChangeCallBack
  def on_job_submitted(change_list)
    @logger.debug "Current jobs: #{@job_list.keys}"
    @status_checker.register_job(change_list) # Must make up entry before rescheduling...
    reschedule
    @status_checker.require_recollect_status  # Validate zombie and wake up idle workers
    return
  end

  def on_job_deleted(change_list)
    # Clear the entry in @job_worker_queues[job_id]
    # Release nodes first
    change_list.each do |job_id|
      @status_checker.delete_job_from_logging(job_id)   # Can't make this an callback in status checker for dependency
      @logger.info "Unregistered #{job_id} from status checker"
    end
    reschedule
    @status_checker.require_recollect_status  # Validate zombie and wake up idle workers
    return
  end
end

module Dispatcher::DispatcherClientInterface
  def register_client
    client_id = SecureRandom.uuid
    @client_job_list[client_id] = []
    @logger.info "Client #{client_id} registered."
    @msg_service_server.register(client_id)
    @logger.info "Message service of client #{client_id} registered."
    return client_id
  end

  def unregister_client(client_id)
    @logger.info "Client #{client_id} unregistered."
    return
  end

  def generate_job_id(job_list, client_id)
    return job_list.map{|job| SecureRandom.uuid}
  end

  def submit_jobs(job_id_table, client_id)
    @logger.info "Job submitted: #{job_id_table.keys}"
    @client_job_list[client_id] += job_id_table.keys # Put it here for callback does not depend on client_id
    @job_list.merge!(job_id_table)
    return job_id_table.keys  # Returning a UUID list stands for acceptance
  end

  def delete_job(jobs, client_id)
    raise ArgumentError if jobs == nil || client_id == nil
    jobs.is_a? Array or jobs = [jobs]
    raise ArgumentError if !(jobs - @client_job_list[client_id]).empty?
    jobs.each{|job_id|@job_list.delete(job_id)}
    @client_job_list[client_id].reject!{|job_id| jobs.include? job_id}
    @logger.debug "Current jobs: #{@job_list.keys}"
    return
  end

  def redo_task(job_id)
    @job_list[job_id].task_redo
    @logger.warn "A task of #{job_id} needs redo, reschedule"
    reschedule
    return
  end

  def task_sent(job_id)
    @job_list[job_id].task_sent
    # TODO: Release tail ones!!!
    return
  end

  def on_job_done(job_id)
    @logger.info "#{job_id} is done"
    @job_list.delete(job_id)
    return
  end

  def tell_worker_down_detected
    @status_checker.require_recollect_status
  end
end

module Dispatcher::DispatcherWorkerInterface 
  # Returns next job assignment
  def on_worker_available(worker)
    @logger.info "Worker #{worker} is available"
    next_job_assigned = nil
    @resource_mutex.synchronize do
      next_job_assigned = @schedule_manager.worker_job_table[worker]
      next if next_job_assigned == nil
      assign_worker_to_job(worker, next_job_assigned)
    end
    return next_job_assigned
  end
end

module Dispatcher::MessageServiceServerDelegator include MessageService::Server
  def get_clients()
    return @msg_service_server.get_clients()
  end

  def register(client_id)
    return @msg_service_server.register(client_id)
  end

  def unregister(client_id)
    return @msg_service_server.unregister(client_id)
  end

  def push_message(client_id, message)
    return @msg_service_server.push_message(client_id, message)
  end

  def broadcast_message(message)
    return @msg_service_server.broadcast_message(message)
  end

  def get_message(client_id, timeout_limit=5)
    return @msg_service_server.get_message(client_id, timeout_limit)
  end
end
