require 'thread'
require 'securerandom'

require_relative 'base_server'
require_relative 'decision_maker'
require_relative 'common/read_write_lock_hash'
class Dispatcher < BaseServer

  class JobList < ReadWriteLockHash
    def initialize(logger)
      super()
      @subscribe_list_mutex = Mutex.new
      @job_list_subscribers = []
      @merge_mutex = Mutex.new
      @logger = logger
    end

    module JobListChangeSubscriber
      # Observer pattern
      def subscribe_job_list_change(subscriber)
        @subscribe_list_mutex.synchronize {@job_list_subscribers << subscriber}
      end
      def publish_job_submitted(job_id_list)
        @job_list_subscribers.each {|subscriber| subscriber.on_job_submitted job_id_list}
      end
      def publish_job_deleted(job_id)
        @job_list_subscribers.each {|subscriber| subscriber.on_job_deleted job_id}
      end
    end
    include JobListChangeSubscriber

    # TODO: Hook notifier on writing methods
    # TODO: Refactor the hook shit, there may be something fancy in ruby to do so...
    def []=(job_id, job)
      super
      publish_job_submitted [job_id]
    end

    def delete(job_id)
      super
      publish_job_deleted job_id
    end

    def merge!(to_add, &block)
      super
      publish_job_submitted @underlying_hash.keys
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
      @logger = arg[:logger]
    end

    def schedule_job()
      @logger.info 'Updating schedule'
      @lock.with_write_lock {
        @job_worker_table = @decision_maker.schedule_job(@job_list, @status_checker.worker_status)  # {job_id => [worker1, worker2...]}
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
  end

  attr_writer :status_checker, :decision_maker

  def initialize(arg={})
    super arg[:logger]
    @resource_mutex = Mutex.new
    @job_worker_queues = ReadWriteLockHash.new
    @job_list = JobList.new @logger
    @job_list.subscribe_job_list_change(self)
    @status_checker = arg[:status_checker]
    @decision_maker = arg[:decision_maker]
    @schedule_manager = ScheduleManager.new @job_list, 
      :status_checker => @status_checker,
      :decision_maker => @decision_maker,
      :logger => @logger
  end

  # Client APIs
  def submit_jobs(job_list)
    job_id_table = Hash[job_list.map {|job| [SecureRandom.uuid, job]}]
    @logger.info "Job submitted: #{job_id_table.keys}"
    @job_list.merge! job_id_table
    @logger.debug "Current jobs: #{@job_list.keys}"
    return job_id_table.keys  # Returning a UUID list stands for acceptance
  end

  def require_worker(job_id)
    # TODO: what if queue popped but not used? ====> more communications
    @logger.info "Worker requirement for #{job_id} issued"
    worker = @job_worker_queues[job_id].pop()
    @logger.info "Worker #{worker} assigned to #{job_id}"
    return worker
  end

  def job_done(job_id)
    @job_list.delete(job_id)
  end

  # Worker APIs
  def on_worker_available(worker)
    @logger.info "Worker #{worker} is available"
    @resource_mutex.synchronize {
      next_job_assigned = @schedule_manager.worker_job_table[worker]
      p @schedule_manager.worker_job_table
      p worker
      p next_job_assigned
      return unless next_job_assigned 
      @job_worker_queues[next_job_assigned].push(worker)
      @status_checker.occupy_worker worker
    }
  end


  # General APIs
  def worker_status()
    # TODO: implement this
    raise NotImplementedError
  end
  def worker_uri(worker)
    return @status_checker.worker_uri worker
  end

  module JobListChangeObserver
    # Observer callbacks
    def on_job_submitted(job_id_list)
      job_id_list.each {|job_id| @job_worker_queues[job_id] = Queue.new}
      # TODO: might need to refactor to observers...
      @schedule_manager.schedule_job
      @status_checker.collect_status
      p @status_checker.worker_status
      p @status_checker.worker_status.select{|w,s|s == Worker::STATUS::AVAILABLE}
      @status_checker.worker_status.select{|w,s|s == Worker::STATUS::AVAILABLE}.each do |w,s| 
        on_worker_available(w)
      end
    end

    def on_job_deleted(job_id)
      # Clear the entry in @job_worker_queues[job_id]
      # Release nodes first
      @resource_mutex.synchronize {
        until @job_worker_queues[job_id].empty? do
          worker = @job_worker_queues[job_id].pop
          @status_checker.release worker
        end
        @job_worker_queues.delete(job_id)
      }
      # TODO: might need to refactor to observers...
      @schedule_manager.schedule_job
      @status_checker.collect_status
      @status_checker.worker_status.select{|w,s|s == Worker::STATUS::AVAILABLE}.each do |w,s| 
        on_worker_available(w)
      end
    end
  end
  include JobListChangeObserver


end
