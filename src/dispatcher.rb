require "event_aggregator"
require 'thread'
require 'timeout'
require 'securerandom'

require_relative 'base_server'
require_relative 'decision_maker'
require_relative 'job'
require_relative 'message_service'
require_relative 'common/rwlock_hash'

class Dispatcher < BaseServer
  include EventAggregator::Listener
  attr_writer :status_checker, :decision_maker

  class JobList < ReadWriteLockHash
    module JobListChangeMessage
      class JobChange
        attr_reader :jobs
        def initialize(j)
          j.is_a?(Array) or j = [j]
          @jobs = j
        end
      end
      class JobSubmission < JobChange;end
      class JobDeletion < JobChange; end
    end
    def initialize(logger)
      super()
      @subscribe_list_mutex = Mutex.new
      @job_list_subscribers = []
      @merge_mutex = Mutex.new
      @logger = logger
    end

    ## Observer pattern
    #def subscribe_job_list_change(subscriber)
    #  @subscribe_list_mutex.synchronize {@job_list_subscribers << subscriber}
    #end
    #def publish_job_submitted(job_id_list)
    #  @job_list_subscribers.each {|subscriber| subscriber.on_job_submitted job_id_list}
    #end
    #def publish_job_deleted(job_id)
    #  @logger.info "Publishing deletion of #{job_id}"
    #  @job_list_subscribers.each {|subscriber| subscriber.on_job_deleted job_id}
    #end

    def []=(job_id, job)
      super
      EventAggregator::Message.new(JobSubmission, JobSubmission.new(job_id)).publish
    end

    def delete(job_id)
      super  # The job_id must be passed if super is not the first statement....
      EventAggregator::Message.new(JobDeletion, JobDeletion.new(job_id)).publish
    end

    def merge!(jobs)
      super
      EventAggregator::Message.new(JobSubmission, JobSubmission.new(jobs.keys)).publish
      return self
    end
  end

  class ScheduleManager
    include EventAggregator::Listener
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
      #@job_list.subscribe_job_list_change(self)
      message_type_register(JobList::JobListChangeMessage::JobDeletion, on_job_deleted)
    end

    def schedule_job()
      @logger.info 'Updating schedule'
      @lock.with_write_lock do
        job_running_time = @status_checker.job_running_time
        worker_avg_running_time = @status_checker.worker_avg_running_time

        # {job_id => [worker1, worker2...]}
        cloned_job_list = Hash.new.merge(Marshal.load(Marshal.dump(@job_list)))
        workers_alive = @status_checker.worker_status.reject{|w,s| s == Worker::STATUS::DOWN}
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

  def initialize(arg={})
    super arg[:logger]
    @resource_mutex = Mutex.new
    @job_worker_queues = ReadWriteLockHash.new
    @job_list = JobList.new @logger
    @status_checker = arg[:status_checker]
    @decision_maker = arg[:decision_maker]
    @schedule_manager = ScheduleManager.new(@job_list,
                                            :status_checker => @status_checker,
                                            :decision_maker => @decision_maker,
                                            :on_schedule_callback => method(:clear_job_worker_queue),
                                            :logger => @logger
                                           )
    #@job_list.subscribe_job_list_change(self) # Make sure it subscribes after schedule manager
    message_type_register(JobList::JobListChangeMessage::JobSubmission, on_job_submitted)
    message_type_register(JobList::JobListChangeMessage::JobDeletion, on_job_deleted)
    @msg_service_server = MessageService::BasicServer.new
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

  def worker_status()
    # TODO: implement this
    raise NotImplementedError
  end
  def worker_uri(worker)
    return @status_checker.worker_uri(worker)
  end

  def log_job_worker_queue
    queue_status = @job_worker_queues.map{|j,wq| [j, "#{wq.size} wrks, #{wq.num_waiting} waiting"]}
    @logger.warn "Current queue status: #{queue_status}"
  end

end

module DispatcherJobListChangeCallBack
  def on_job_deleted(job_change_msg)
    # Clear the entry in @job_worker_queues[job_id]
    # Release nodes first
    job_change_msg.jobs.each do |job_id|
      @logger.info "Remove #{job_id} worker queue"
      @job_worker_queues.delete job_id
      @logger.info "Unregistering #{job_id} from status checker"
      @status_checker.delete_job job_id # Can't make this an observer in status checker for dependency
    end
  end
end

module DispatcherClientInterface
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

  def submit_jobs(job_list)
    job_id_table = Hash[job_list.map {|job| [SecureRandom.uuid, job]}]
    @logger.info "Job submitted: #{job_id_table.keys}"
    job_id_table.keys.each do |job_id|
      # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      # !!!NEVER directly cover it with new queue, threads are waiting on the old queues!!!
      # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      @job_worker_queues[job_id] = Queue.new
      @status_checker.register_job job_id # Can't make this an observer in status checker for dependency
    end
    @logger.info "Created job worker queue for: #{job_id_table.keys}"
    @job_list.merge! job_id_table
    @logger.debug "Current jobs: #{@job_list.keys}"
    @logger.debug "Current schedule: #{@schedule_manager.job_worker_table}"
    return job_id_table.keys  # Returning a UUID list stands for acceptance
  end

  def require_worker(job_id)
    # TODO: what if queue popped but not used? ====> more communications
    @logger.info "#{job_id} requires a worker"
    worker = @job_worker_queues[job_id].pop()
    @logger.info "#{job_id} gets worker #{worker}"
    return worker
  end

  def task_redo(job_id)
    @job_list[job_id].task_redo
  end

  def task_sent(job_id)
    @job_list[job_id].task_sent
  end

  def job_done(job_id)
    @logger.info "#{job_id} is done"
    @job_list.delete(job_id)
  end
end

module DispatcherWorkerInterface
  # Worker APIs
  def on_task_done(worker, client_id, job_id)
    # TODO implement this
  end

  def on_worker_available(worker)
    @logger.info "Worker #{worker} is available"
    @resource_mutex.synchronize do
      next_job_assigned = @schedule_manager.worker_job_table[worker]
      @logger.debug "Worker #{worker} will be assigned to #{next_job_assigned.inspect}"
      return unless @job_worker_queues[next_job_assigned]
      waiting_workers_of_next_job_assigned = @job_worker_queues[next_job_assigned].size
      waiting_threads_of_next_job_assigned = @job_worker_queues[next_job_assigned].num_waiting
      @logger.debug "Worker #{worker} pushed to the queue of #{next_job_assigned.inspect}"
      @job_worker_queues[next_job_assigned].push(worker)
      @status_checker.occupy_worker worker
      @logger.debug "#{next_job_assigned} queue has #{waiting_workers_of_next_job_assigned} waiting workers, #{waiting_threads_of_next_job_assigned} threads waiting it"
    end
  end
end

module MessageServiceServerDelegator include MessageService::Server
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

class Dispatcher
  include DispatcherJobListChangeCallBack
  include DispatcherClientInterface
  include DispatcherWorkerInterface
  include MessageServiceServerDelegator
end
