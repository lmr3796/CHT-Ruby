require 'drb'
require 'eventmachine'

require_relative 'base_server'
require_relative 'worker'
require_relative 'common/rwlock'
require_relative 'common/rwlock_hash'

class StatusChecker < BaseServer
  module WorkerInterface;end
  include WorkerInterface

  def job_running_time()
    @lock.with_read_lock{return Hash[@job_running_time]}
  end
  def worker_avg_running_time()
    @lock.with_read_lock{return Hash[@worker_avg_running_time]}
  end
  def worker_status
    @lock.with_read_lock{return Hash[@worker_status_table]}
  end
  def worker_uri(worker)
    return @worker_table[worker].instance_variable_get("@uri")
  end
  def initialize(worker_table={},arg={})
    super arg[:logger]
    # TODO: make up a worker table
    @lock = ReadWriteLock.new
    @job_running_time = ReadWriteLockHash.new
    @worker_table = Hash[worker_table]
    @worker_status_table = Hash[worker_table.map{|w_id, w| [w_id, Worker::STATUS::UNKNOWN]}]
    @worker_avg_running_time = Hash[worker_table.map{|w_id, w| [w_id, nil]}]
    @dispatcher = arg[:dispatcher]
    collect_status
    if arg[:update_period]
      Thread.new do
        EventMachine.run do
          EventMachine.add_periodic_timer(arg[:update_period]) do
            collect_status
            @logger.info 'Asked to reschedule'
            begin
              @dispatcher.reschedule
            rescue => e
              @logger.error "Error contacting dispatcher for reschedule"
              @logger.debug e.message
              @logger.debug e.backtrace.join("\n")
            end
          end
        end
      end
    end
  end
  def collect_status(workers=@worker_table.keys)
    @logger.info "Collecting status"
    @lock.with_write_lock do
      workers.each do |w|
        status = nil
        avg_time = nil
        begin
          status = @worker_table[w].status
          avg_time = @worker_table[w].avg_running_time
        rescue => e
          @logger.warn "Exception #{e} when checking status of worker #{w}"
          @logger.debug e.to_s
          status = Worker::STATUS::DOWN
        ensure
          @logger.info "#{w} is #{status}"
          @worker_status_table[w] = status 
          @worker_avg_running_time[w] = avg_time
        end
      end
    end
    workers.select{|w| @worker_status_table[w] == Worker::STATUS::AVAILABLE}.each do |w|
      begin
        @dispatcher.on_worker_available w
      rescue => e
        @logger.error "Error reaching dispatcher"
        @logger.debug e.backtrace
      end
    end
  end

  def register_job(job_id)
    @lock.with_write_lock do
      @logger.info "Job #{job_id} registered for recording"
      @job_running_time[job_id] = []
    end
  end
  def delete_job(job_id)
    @lock.with_write_lock do
      @logger.info "Job #{job_id} removed from recording"
      @job_running_time.delete job_id
    end
  end

  def log_running_time(job_id, time)
    raise ArgumentError unless time.is_a? Float
    @lock.with_write_lock{@job_running_time[job_id] << time} # Necessary even it's a ReadWriteLockHash
    return 
  end

end

module StatusChecker::WorkerInterface
  def register_worker(worker)
    @lock.with_write_lock do
      @worker_status_table[worker] = @worker_table[worker].status = Worker::STATUS::AVAILABLE
      @logger.info "Worker: #{worker} registered; set to AVAILABLE"
    end
    @logger.info 'Asked to reschedule'
    begin
      @dispatcher.reschedule
    rescue => e
      @logger.error "Error reaching dispatcher"
      @logger.debug e.message
      @logger.debug e.backtrace.join("\n")
    end
    @dispatcher.on_worker_available(worker)
  end

  def release_worker(worker, notify=true)
    @lock.with_write_lock do
      @worker_status_table[worker] = @worker_table[worker].status = Worker::STATUS::AVAILABLE
      @logger.info "Released worker: #{worker}"
    end
    @dispatcher.on_worker_available(worker) if notify
  end
 
  def occupy_worker(worker)
    @lock.with_write_lock do
      @worker_status_table[worker] = @worker_table[worker].status = Worker::STATUS::OCCUPIED
      @logger.info "Occupied worker: #{worker}"
    end
  end

  def worker_running(worker)
    @lock.with_write_lock do
      @worker_status_table[worker] = @worker_table[worker].status = Worker::STATUS::BUSY
      @logger.info "Mark running worker: #{worker}"
    end
  end

  def on_task_done(worker, task_id, job_id, client_id)
    release_worker(worker)
  end
end
