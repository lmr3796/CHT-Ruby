require 'observer'
require 'thread'
require 'timeout'
require 'securerandom'

require_relative 'base_server'
require_relative 'decision_maker'
require_relative 'job'
require_relative 'message_service'
require_relative 'common/rwlock_hash'

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

class Dispatcher::JobList < ReadWriteLockHash
  attr_reader :deletion_observerable, :submission_observerable
  def initialize(logger)
    super()
    @logger = logger
    @deletion = Observable.new
    @submission = Observable.new
  end

  def []=(job_id, job)
    super
    submission.notify_observers([job_id])
  end

  def delete(job_id)
    super  # The job_id must be passed if super is not the first statement....
    deletion.notify_observers([job_id])
  end

  def merge!(jobs)
    super
    submission.notify_observers(jobs)
    return self
  end
end

class Dispatcher::ClientJobList < ReadWriteLockHash
  def get_client_by_job(job_id)
    each{|c, jl| return c if jl.include?(job_id)}
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
    @decision_maker = arg[:decision_maker]
    @status_checker = arg[:status_checker]
    @on_schedule_callback = arg[:on_schedule_callback]
    @on_schedule_callback = [@on_schedule_callback] if !@on_schedule_callback.is_a? Array

    @job_list = job_list
    @job_list.submission.add_observer(self, :on_job_submitted)
    @job_list.deletion.add_observer(self, :on_job_deleted)
  end

  def schedule_job()
    @logger.info 'Updating schedule'
    @lock.with_write_lock do
      job_running_time = @status_checker.job_running_time
      worker_avg_running_time = @status_checker.worker_avg_running_time

      cloned_job_list = Hash.new.merge(Marshal.load(Marshal.dump(@job_list)))
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
        workers.each do |worker|
          @worker_job_table[worker] = job_id
        end
      end
    end
    @on_schedule_callback.each{|c|c.call}
    @logger.info 'Updated schedule successfully'
  end

  # Observer callbacks
  def on_job_submitted(job_id_list)
    schedule_job
  end

  def on_job_deleted(job_id)
    @worker_job_table.delete_if{|w,j| j == job_id}
    schedule_job
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

    @schedule_manager = ScheduleManager.new(@job_list,
                                            :status_checker => @status_checker,
                                            :decision_maker => @decision_maker,
                                            :on_schedule_callback => method(:clear_job_worker_queue),
                                            :logger => @logger
                                           )
    @client_job_list = ClientJobList.new

    @job_list = JobList.new @logger
    @job_list.submission.add_observer(self, :on_job_submitted)
    @job_list.deletion.add_observer(self, :on_job_deleted)
  end

  def clear_job_worker_queue
    @job_worker_queues.values.each{|q| q.clear}
    occupied_workers = @status_checker.worker_status.select{|_,s|s == Worker::STATUS::OCCUPIED}
    occupied_workers.each{|w,_|@status_checker.release_worker(w)}
  end
  private :clear_job_worker_queue

  # General APIs
  def reschedule()
    @schedule_manager.schedule_job
  end

  def worker_uri(worker)
    return @status_checker.worker_uri(worker)
  end

  def log_job_worker_queue
    queue_status = @job_worker_queues.map{|j,wq| [j, "#{wq.size} wrks, #{wq.num_waiting} waiting"]}
    @logger.warn "Current queue status: #{queue_status}"
  end

end

module Dispatcher::DispatcherJobListChangeCallBack
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
  end
end

module Dispatcher::DispatcherClientInterface
  def register_client
    client_id = SecureRandom.uuid
    @logger.info "Client #{client_id} registered."
    @msg_service_server.register(client_id)
    @logger.info "Message service of client #{client_id} registered."
    return client_id
  end

  def unregister_client(client)
    @client_message_queue[client.uuid].clear
    @client_message_queue.delete client.uuid
    @logger.info "Client #{client.uuid} unregistered."
  end

  def submit_jobs(job_list, client_id)
    job_id_table = Hash[job_list.map {|job| [SecureRandom.uuid, job]}]
    @client_list[client_id] += job_id_table.keys
    @logger.info "Job submitted: #{job_id_table.keys}"
    @job_list.merge! job_id_table
    @logger.debug "Current jobs: #{@job_list.keys}"
    @logger.debug "Current schedule: #{@schedule_manager.job_worker_table}"
    return job_id_table.keys  # Returning a UUID list stands for acceptance
  end

  def delete_job(jobs)
    jobs.is_a? Array or jobs = [jobs]
    jobs.each{|job_id|@job_list.delete(job_id)}
    @client_job_list[client_id].reject!{|job_id| jobs.include? job_id}
  end

  def task_redo(job_id)
    @job_list[job_id].task_redo
  end

  def task_sent(job_id)
    @job_list[job_id].task_sent
  end

  def on_job_done(job_id)
    @logger.info "#{job_id} is done"
    @job_list.delete(job_id)
  end
end

module Dispatcher::DispatcherWorkerInterface
  # Worker APIs
  def on_task_done(worker, task_id, job_id, client_id)
    push_message(client_id, MessageService::Message.new(:task_result_available, :job_id=>job_id, :task_id=>task_id))
  end

  def on_worker_available(worker)
    @logger.info "Worker #{worker} is available"
    @resource_mutex.synchronize do
      next_job_assigned = @schedule_manager.worker_job_table[worker]
      @logger.debug "Worker #{worker} will be assigned to #{next_job_assigned.inspect}"
      return unless @job_worker_queues[next_job_assigned]
      push_message(@client_list.get_client_by_job(next_job_assigned), MessageService::Message.new(:worker_available))
    end
  end
end

module Dispatcher::MessageServiceServerDelegator include MessageService::Server
  def get_clients()
    @msg_service_server.get_clients()
  end

  def register(client_id)
    @msg_service_server.register(client_id)
  end

  def unregister(client_id)
    @msg_service_server.unregister(client_id)
  end

  def push_message(client_id, message)
    @msg_service_server.push_message(client_id, message)
  end

  def broadcast_message(message)
    @msg_service_server.broadcast_message(message)
  end

  def get_message(client_id, timeout_limit=5)
    @msg_service_server.get_message(client_id, timeout_limit)
  end
end
