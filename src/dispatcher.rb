require 'thread'
require 'securerandom'

require_relative 'decision_maker'
require_relative 'common/read_write_lock_hash'
class Dispatcher

  class JobList < ReadWriteLockHash
    def initialize()
      @subscribe_list_mutex = Mutex.new
      @job_list_subscribers = []
      @merge_mutex = Mutex.new
      super
    end

    # Observer's pattern
    def subscribe_job_list_change(subscriber)
      @subscribe_list_mutex.synchronize {@job_list_subscribers << subscriber}
    end
    def publish_job_submitted(job_id_list)
      @job_list_subscribers.each {|subscriber| subscriber.on_job_submitted job_id_list}
    end
    def publish_job_deleted(job_id)
      @job_list_subscribers.each {|subscriber| subscriber.on_job_deleted job_id}
    end

    # TODO: Hook notifier on writing methods
    # TODO: Refactor the hook shit, there may be something fancy in ruby to do so...
    def []=(job_id, job)
      super
      publish_job_add [job_id]
    end

    def delete(job_id)
      super
      publish_job_deleted job_id
    end

    def merge!(to_add)
      job_id_list = nil
      @read_write_lock.with_write_lock{
        job_id_list = to_add.keys - @underlying_hash.keys
        @underlying_hash.merge!(to_add, &block)
      }
      publish_job_add job_id_list
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
        @job_worker_table = decision_maker.schedule_job(@job_list)  # {job_id => [worker1, worker2...]}
        @worker_job_table = ReadWriteLockHash.new
        @job_worker_table.keys.each { |job_id|
          # TODO: maybe only update the table with those changed
          workers = @job_worker_table[job_id]
          workers.each { |worker|
            @worker_job_table[worker] = job_id
          }
        }
      }
    end

    # Observer call back
    def on_job_submitted(job_id_list)
      # TODO: implement this...
      raise NotImplementedError
    end
    def on_job_deleted()
      # TODO: implement this...
      raise NotImplementedError
    end

  end

  attr_writer :status_checker, :decision_maker

  def initialize(arg={})
    raise ArgumentError.new(arg.to_s) if !(arg.keys-[:status_checker, :decision_maker]).empty?
    @job_list = JobList.new
    @job_list.subscribe_job_list_change(self)
    @schedule_manager = ScheduleManager.new(@job_list)
    @job_worker_queues = ReadWriteLockHash.new
    @resource_mutex = Mutex.new
    @status_checker = arg[:status_checker]
    @decision_maker = arg[:decision_maker]
  end

  # Client APIs
  def submit_jobs(job_list)
    job_id_table = Hash[job_list.map {|job| [SecureRandom.uuid, job]}]
    @resource_mutex.synchronize {
      @job_list.merge! job_id_table
    }
    return job_id_list  # Returning a UUID list stands for acceptance
  end

  def require_worker(job_id)
    # TODO: what if queue popped but not used? ====> more communications
    return @job_worker_queues[job_id].pop()
  end

  def job_done(job_id)
    @job_list.delete(job_id)
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
  def on_job_submitted(job_id_list)
    job_id_list.each { |job_id|
      @job_worker_table[job_id].each { |worker|
        @job_worker_queues[job_id] = Queue.new
      }
    }
  end

  def on_job_deleted(job_id)
    # Clear the entrie in @job_worker_queues[job_id]
    # Release nodes first
    @resource_mutex.synchronize {
      until @job_worker_queues[job_id].empty? do
        worker = @job_worker_queues[job_id].pop
        @status_checker.release worker
      end
      @job_worker_queues.delete(job_id)
    }
  end

  def worker_status()
    # TODO: implement this
    raise NotImplementedError
  end

end
