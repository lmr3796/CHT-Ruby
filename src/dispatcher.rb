require 'thread'
require 'timeout'
require 'securerandom'

require_relative 'base_server'
require_relative 'decision_maker'
require_relative 'common/read_write_lock_hash'

class Dispatcher < BaseServer
  attr_writer :status_checker, :decision_maker
  attr_reader :client_message_queue

  class JobList < ReadWriteLockHash
    def initialize(logger)
      super()
      @subscribe_list_mutex = Mutex.new
      @job_list_subscribers = []
      @merge_mutex = Mutex.new
      @logger = logger
    end

    # Observer pattern
    def subscribe_job_list_change(subscriber)
      @subscribe_list_mutex.synchronize {@job_list_subscribers << subscriber}
    end
    def publish_job_submitted(job_id_list)
      @job_list_subscribers.each {|subscriber| subscriber.on_job_submitted job_id_list}
    end
    def publish_job_deleted(job_id)
      @logger.info "Publishing deletion of #{job_id}"
      @job_list_subscribers.each {|subscriber| subscriber.on_job_deleted job_id}
    end

    # TODO: Hook notifier on writing methods
    # TODO: Refactor the hook shit, there may be something fancy in ruby to do so...
    def []=(job_id, job)
      super
      publish_job_submitted [job_id]
    end

    def delete(*args)
      super  # The job_id must be passed if super is not the first statement....
      job_id = args[0]
      publish_job_deleted job_id
    end

    def merge!(*args)
      super
      publish_job_submitted(keys)
      return self
    end

  end

  class ScheduleManager
    attr_reader :job_worker_table, :worker_job_table
    attr_writer :status_checker, :decision_maker
    def initialize(job_list, arg={})
      @lock = ReadWriteLock.new
      @worker_job_table = {}
      @job_worker_table = {}
      @decision_maker = arg[:decision_maker]
      @status_checker = arg[:status_checker]
      @job_list = job_list
      @job_list.subscribe_job_list_change(self)
      @logger = arg[:logger]
    end

    def schedule_job()
      @logger.info 'Updating schedule'
      @lock.with_write_lock {
        worker_can_be_scheduled = @status_checker.worker_status.delete_if{|w,s| s == Worker::STATUS::DOWN} # Don't schedule on downed workers
        job_running_time = @status_checker.job_running_time
        worker_avg_running_time = @status_checker.worker_avg_running_time

        # {job_id => [worker1, worker2...]}
        cloned_job_list = Hash.new.merge Marshal.load(Marshal.dump(@job_list))
        @job_worker_table = @decision_maker.schedule_job cloned_job_list, worker_can_be_scheduled,
          :job_running_time=>job_running_time, :worker_avg_running_time => worker_avg_running_time


        @worker_job_table = ReadWriteLockHash.new
        @job_worker_table.keys.each { |job_id|
          # TODO: maybe only update the table with those changed
          workers = @job_worker_table[job_id]
          workers.each { |worker|
            @worker_job_table[worker] = job_id
          }
        }
      }
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
    @schedule_manager = ScheduleManager.new @job_list,
      :status_checker => @status_checker,
      :decision_maker => @decision_maker,
      :logger => @logger
    @job_list.subscribe_job_list_change(self) # Make sure it subscribes after schedule manager
    @client_message_queue = ReadWriteLockHash.new
  end

  # Client APIs
  def register_client
    client_id = SecureRandom.uuid
    @client_message_queue[client_id] = Queue.new
    @logger.info "Client #{client_id} registered."
    return client_id
  end
  def unregister_client(client)
    @client_message_queue[client.uuid].clear
    @client_message_queue.delete client.uuid
    @logger.info "Client #{client.uuid} unregistered."
  end
  def push_message(client_id, message)
    @client_message_queue[client_id] << message
  end
  def get_message(client_id, timeout_limit=5)
    msg = []
    Timeout::timeout(timeout_limit) do
      loop do # collect all as a batch
        msg << @client_message_queue[client_id].pop
        break if @client_message_queue[client_id].empty?
      end
    end
  rescue Timeout::Error  #This rescue is very necessary since DRb seems to catch it outside :P
  ensure
    return msg
  end

  def submit_jobs(job_list)
    job_id_table = Hash[job_list.map {|job| [SecureRandom.uuid, job]}]
    @logger.info "Job submitted: #{job_id_table.keys}"
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
  def one_task_done(job_id)
    @job_list[job_id].one_task_done
  end
  def job_done(job_id)
    @logger.info "#{job_id} is done"
    @job_list.delete(job_id)
  end
  def reschedule()
    @schedule_manager.schedule_job
  end

  # Worker APIs
  def on_task_done(worker, client_id, job_id)
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

  # General APIs
  def worker_status()
    # TODO: implement this
    raise NotImplementedError
  end
  def worker_uri(worker)
    return @status_checker.worker_uri worker
  end

  def log_job_worker_queue
    queue_status = @job_worker_queues.map{|k,v| [k, "#{v.size} wrks, #{v.num_waiting} waiting"]}
    @logger.warn "Current queue status: #{queue_status}"
  end

  # Observer callbacks
  def on_job_submitted(job_id_list)
    job_id_list.each do |job_id|
      # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      # !!!NEVER directly cover it with new queue, threads are waiting on the old queues!!!
      # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      if @job_worker_queues.has_key? job_id
        @job_worker_queues[job_id].clear
      else
        @job_worker_queues[job_id] = Queue.new
        @status_checker.register_job job_id # Can't make this an observer in status checker for dependency
      end
    end
    # TODO: might need to refactor to observers...
    @logger.info "Collecting status from status checker"
    @status_checker.collect_status
    worker_status = @status_checker.worker_status
    @logger.info "Current worker status: #{worker_status}"
    worker_status.select{|w,s|s == Worker::STATUS::AVAILABLE}.each do |w,s|
      on_worker_available(w)
    end
  end

  def on_job_deleted(job_id)
    # Clear the entry in @job_worker_queues[job_id]
    # Release nodes first
    @logger.debug "Clearing #{job_id} worker queue"
    until @job_worker_queues[job_id].empty? do
      worker = @job_worker_queues[job_id].pop
      @status_checker.release_worker worker
    end
    # FIXME: very possible bug here
    @logger.warn "Remove #{job_id} worker queue"
    @job_worker_queues.delete job_id
    # TODO: might need to refactor to observers...
    @schedule_manager.schedule_job
    @status_checker.delete_job job_id # Can't make this an observer in status checker for dependency
    @status_checker.collect_status
    @status_checker.worker_status.select{|w,s|s == Worker::STATUS::AVAILABLE}.each do |w,s|
      on_worker_available(w)
    end
  end

end
