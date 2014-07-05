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

class Dispatcher::JobList < ReadWriteLockHash
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
    each{|c, jl| return c if jl.include?(job_id)}
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
    @on_schedule_callback = arg[:on_schedule]
    return
  end

  def schedule_job()
    @logger.info 'Updating schedule'
    @lock.with_write_lock do
      job_running_time = @status_checker.job_running_time
      worker_avg_running_time = @status_checker.worker_avg_running_time
      cloned_job_list = Hash.new.merge(@job_list)
      workers_alive = @status_checker.worker_status.reject{|w,s| s == Worker::STATUS::DOWN}
      # Schedule result:  {job_id => [worker1, worker2...]}
      @job_worker_table = @decision_maker.schedule_job(
        cloned_job_list,
        workers_alive, # Don't schedule on downed workers
        :job_running_time=>job_running_time,
        :worker_avg_running_time => worker_avg_running_time
      )

      @worker_job_table = ReadWriteLockHash.new
      @job_worker_table.keys.each do |job_id|
        workers = @job_worker_table[job_id]
        workers.each{|worker| @worker_job_table[worker] = job_id}
      end
    end
    @on_schedule_callback.call(@job_worker_table) if @on_schedule_callback.respond_to? :call
    @logger.info 'Updated schedule successfully'
    @logger.debug "Current schedule: #{@job_worker_table}"
    return
  end

  # Observer callbacks
  def on_job_submitted(change_list)
    schedule_job
    @status_checker.register_job(change_list)
    return
  end

  def on_job_deleted(change_list)
    @worker_job_table.delete_if{|w,j| change_list.include? job_id}
    schedule_job
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
                                            :on_schedule => method(:on_reschedule),
                                            :logger => @logger)
    @job_list.subscribe(:submission, self, :on_job_submitted)
    @job_list.subscribe(:submission, @schedule_manager, :on_job_submitted)
    @job_list.subscribe(:deletion, self, :on_job_deleted)
    @job_list.subscribe(:deletion, @schedule_manager, :on_job_deleted)
    return
  end

  def on_reschedule(schedule)
    @status_checker.collect_status
    return
  end

  def assign_worker_to_job(worker, job_id)
    @logger.debug "Worker #{worker} will be assigned to #{job_id}"
    job_assignment = [job_id, @client_job_list.get_client_by_job(job_id)]
    job_assignment = Worker::JobAssignment.new(job_id, @client_job_list.get_client_by_job(job_id))
    DRbObject.new_with_uri(worker_uri(worker)).assign_job(job_assignment) # May have to clean this ??
    return
  end

  # General APIs
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

end

module Dispatcher::DispatcherJobListChangeCallBack
  def on_job_submitted(_)
    @logger.debug "Current jobs: #{@job_list.keys}"
    return
  end

  def on_job_deleted(change_list)
    # Clear the entry in @job_worker_queues[job_id]
    # Release nodes first
    change_list.each do |job_id|
      @logger.info "Remove #{job_id} worker queue"
      @job_worker_queues.delete job_id
      @logger.info "Unregistering #{job_id} from status checker"
      @status_checker.delete_job job_id # Can't make this an observer in status checker for dependency
      @logger.info "Unregistering #{job_id} from status checker"
    end
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

  def unregister_client(client)
    @client_message_queue[client.uuid].clear
    @client_message_queue.delete client.uuid
    @logger.info "Client #{client.uuid} unregistered."
    return
  end

  def generate_job_id(job_list, client_id)
    return job_list.map{|job| SecureRandom.uuid}
  end

  def submit_jobs(job_id_table, client_id)
    @logger.info "Job submitted: #{job_id_table.keys}"
    @client_job_list[client_id] += job_id_table.keys # Put it here for callback does not depend on client_id
    @job_list.merge! job_id_table
    return job_id_table.keys  # Returning a UUID list stands for acceptance
  end

  def delete_job(jobs)
    jobs.is_a? Array or jobs = [jobs]
    jobs.each{|job_id|@job_list.delete(job_id)}
    @client_job_list[client_id].reject!{|job_id| jobs.include? job_id}
    return
  end

  def task_redo(job_id)
    @job_list[job_id].task_redo
    reschedule
    return
  end

  def task_sent(job_id)
    @job_list[job_id].task_sent
    return
  end

  def on_job_done(job_id)
    @logger.info "#{job_id} is done"
    @job_list.delete(job_id)
    return
  end
end

module Dispatcher::DispatcherWorkerInterface
  # Worker APIs
  def on_worker_available(worker)
    @logger.info "Worker #{worker} is available"
    @resource_mutex.synchronize do
      next_job_assigned = @schedule_manager.worker_job_table[worker]
      return if next_job_assigned == nil
      assign_worker_to_job(worker, next_job_assigned)
    end
    return
  end
end

module Dispatcher::MessageServiceServerDelegator include MessageService::Server
  def get_clients()
    @msg_service_server.get_clients()
    return
  end

  def register(client_id)
    @msg_service_server.register(client_id)
    return
  end

  def unregister(client_id)
    @msg_service_server.unregister(client_id)
    return
  end

  def push_message(client_id, message)
    @msg_service_server.push_message(client_id, message)
    return
  end

  def broadcast_message(message)
    @msg_service_server.broadcast_message(message)
    return
  end

  def get_message(client_id, timeout_limit=5)
    return @msg_service_server.get_message(client_id, timeout_limit)
  end
end
