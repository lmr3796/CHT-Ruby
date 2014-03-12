require 'thread'
require 'securerandom'

require_relative 'decision_maker'
require_relative 'read_write_lock_hash'
class Dispatcher
  attr_writer :status_checker, :decision_maker

  def initialize(arg={})
    raise ArgumentError.new(arg.to_s) if !(arg.keys-[:status_checker, :decision_maker]).empty?
    @worker_job_table = ReadWriteLockHash.new
    @job_worker_table = ReadWriteLockHash.new
    @job_worker_queues = ReadWriteLockHash.new
    @job_list = ReadWriteLockHash.new
    @table_mutex = Mutex.new
    @status_checker = arg[:status_checker]
    @decision_maker = arg[:decision_maker]
  end

  # Client APIs
  def submit_jobs(job_list)
    uuid_table = Hash[job_list.map {|job| [SecureRandom.uuid, job]}]
    @table_mutex.synchronize {
      @job_list.merge! uuid_table
      reschedule_jobs() 
      uuid_table.keys.each { |uuid|
        @job_worker_table[uuid].each { |worker|
          @job_worker_queues[uuid].push worker if @status_checker.get_status(worker) == StatusChecker::AVAILABLE
        }
      }
    }
    return uuid_list  # Returning a UUID list stands for acceptance
  end

  def require_worker(job_id)
    # TODO: what if queue popped but not used? ====> more communications
    return @job_worker_queues[job_id].pop()
  end

  def job_done(uuid)
    # TODO: Reschedule jobs
    @table_mutex.synchronize {
      @job_list.delete(uuid)
      until @job_worker_queues[uuid].empty? do
        worker = @job_worker_queues[uuid].pop
        @status_checker.release worker
      end
      @job_worker_queues.delete(uuid)
    }
  end

  # Worker APIs
  def on_worker_available(worker)
    @table_mutex.synchronize {
      @job_worker_queues[ @worker_job_table[worker] ].push(worker)
      @statusChecker.occupy_worker worker
    } unless @worker_job_table.has_key? worker 
  end

  # General APIs
  def reschedule_jobs()   # TODO: If this take too long have to make it an asynchronous call
    if @table_mutex.owned?
      @job_worker_table = decision_maker.schedule_jobs(@job_list)
      @worker_job_table = ReadWriteLockHash.new
      @job_worker_table.keys.each { |job_id|
        workers = @job_worker_table[job_id]
        workers.each { |worker|
          @worker_job_table[worker] = job_id
        }
      }
    else
      @table_mutex.synchronize {
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
  end

end
