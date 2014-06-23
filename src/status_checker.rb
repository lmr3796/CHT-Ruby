require 'drb'
require 'eventmachine'

require_relative 'base_server'
require_relative 'worker'
require_relative 'common/read_write_lock'
require_relative 'common/read_write_lock_hash'

class StatusChecker < BaseServer

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
  def register_worker(worker)
    @lock.with_write_lock {
      @worker_status_table[worker] = @worker_table[worker].status = Worker::STATUS::AVAILABLE
      @logger.info "Worker: #{worker} registered; set to AVAILABLE"
    }
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
  def release_worker(worker)
    @lock.with_write_lock {
      @worker_status_table[worker] = @worker_table[worker].status = Worker::STATUS::AVAILABLE
      @logger.info "Released worker: #{worker}"
    }
    @dispatcher.on_worker_available(worker)
  end
  def occupy_worker(worker)
    @lock.with_write_lock {
      @worker_status_table[worker] = @worker_table[worker].status = Worker::STATUS::OCCUPIED
      @logger.info "Occupied worker: #{worker}"
    }
  end
  def worker_running(worker)
    @lock.with_write_lock {
      @worker_status_table[worker] = @worker_table[worker].status = Worker::STATUS::BUSY
      @logger.info "Mark running worker: #{worker}"
    }
  end
  def collect_status(workers=@worker_table.keys)
    @logger.info "Collecting status"
    occupied = []
    @lock.with_write_lock do
      workers.each do |w|
        status = nil
        avg_time = nil
        begin
          avg_time = @worker_table[w].avg_running_time
          status = @worker_table[w].status
          occupied << w if status == Worker::STATUS::OCCUPIED
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

    occupied.each do |w|
      @worker_table[w].set_nice
      if @worker_table[w].nice > 2
        @logger.error "#{w} stucked at occupied, force release"
        @worker_table[w].force_release
      end
    end

    # In case worker is available but not dispatched
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
