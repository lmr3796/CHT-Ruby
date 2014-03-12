require 'thread'
require 'securerandom'

require_relative 'decision_maker'
require_relative 'read_write_lock_hash'
class Dispatcher

  class JobList < ReadWriteLockHash
    def initialize()
      @subscribe_list_mutex = Mutex.new
      @job_list_subscribers = []
      super
    end

    # Observer's pattern
    def subscribe_job_list_change(subscriber)
      @subscribe_list_mutex.synchronize {@job_list_subscribers << subscriber}
    end
    def publish_job_submitted()
      @job_list_subscribers.each {|subscriber| subscriber.on_job_submitted}
    end
    def publish_job_deleted()
      @job_list_subscribers.each {|subscriber| subscriber.on_job_deleted uuid}
    end

    # TODO: Hook notifier on writing methods
    # TODO: Refactor the hook shit, there may be something fancy in ruby to do so...
    def []=(uuid, job)
      super
      publish_job_add
    end

    def delete(uuid)
      super
      publish_job_deleted uuid
    end

    def merge!(hsh)
      super
      publish_job_add
    end

  end

  class ScheduleManager
    def initialize(job_list)
      @lock = ReadWriteLock.new
      @worker_job_table = {}
      @job_worker_table = {}
      @job_list = job_list
      @job_list.subscribe_job_list_change(self)
    end

    def schedule_job()
      @lock.with_write_lock {
        # TODO: coordinate output from decision maker
        @job_worker_table = decision_maker.schedule_jobs(@job_list)
        @worker_job_table = ReadWriteLockHash.new
        @job_worker_table.keys.each { |job_id|
          workers = @job_worker_table[job_id]
          workers.each { |worker|
            @worker_job_table[worker] = job_id
          }
        }
      }
    end

    # Observer call back
    def on_job_submitted()
      # TODO: implement this...
    end
    def on_job_deleted()
      # TODO: implement this...
    end

  end

  attr_writer :status_checker, :decision_maker

  def initialize(arg={})
    raise ArgumentError.new(arg.to_s) if !(arg.keys-[:status_checker, :decision_maker]).empty?
    @job_list = ReadWriteLockHash.new
    @job_list.subscribe_job_list_change(self)
    @schedule_manager = ScheduleManager.new(job_list)
    @job_worker_queues = ReadWriteLockHash.new
    @resource_mutex = Mutex.new
    @status_checker = arg[:status_checker]
    @decision_maker = arg[:decision_maker]
  end

  # Client APIs
  def submit_jobs(job_list)
    uuid_table = Hash[job_list.map {|job| [SecureRandom.uuid, job]}]
    @resource_mutex.synchronize {
      @job_list.merge! uuid_table
    }
    return uuid_list  # Returning a UUID list stands for acceptance
  end

  def require_worker(job_id)
    # TODO: what if queue popped but not used? ====> more communications
    return @job_worker_queues[job_id].pop()
  end

  def job_done(uuid)
    @job_list.delete(uuid)
  end

  # Worker APIs
  def on_worker_available(worker)
    @resource_mutex.synchronize {
      @job_worker_queues[ @worker_job_table[worker] ].push(worker)
      @statusChecker.occupy_worker worker
    } if @worker_job_table.has_key? worker 
  end


  # General APIs

  # Observer callbacks
  def on_job_submitted()
    uuid_table.keys.each { |uuid|
      @job_worker_table[uuid].each { |worker|
        @job_worker_queues[uuid] = Queue.new
      }
    }
  end

  def on_job_deleted(uuid)
    # Clear the entrie in @job_worker_queues[uuid]
    # Release nodes first
    @resource_mutex.synchronize {
      until @job_worker_queues[uuid].empty? do
        worker = @job_worker_queues[uuid].pop
        @status_checker.release worker
      end
      @job_worker_queues.delete(uuid)
    }
  end

end
