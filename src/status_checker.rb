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
    return
  end

  def worker_avg_running_time()
    @lock.with_read_lock{return Hash[@worker_avg_running_time]}
    return
  end

  def worker_status
    @lock.with_read_lock{return Hash[@worker_status_table]}
    return
  end

  def worker_uri(worker)
    @worker_server_table.has_key? worker or raise ArgumentError, "No worker #{worker} found"
    return @worker_server_table[worker].instance_variable_get("@uri")
  end

  def initialize(worker_table={},arg={})
    super arg[:logger]
    # TODO: make up a worker table
    @lock = ReadWriteLock.new
    @job_running_time = ReadWriteLockHash.new
    @worker_server_table = Hash[worker_table]
    @worker_status_table = Hash[worker_table.map{|w_id, w| [w_id, Worker::STATUS::UNKNOWN]}]
    @worker_avg_running_time = Hash[worker_table.map{|w_id, w| [w_id, nil]}]
    @dispatcher = arg[:dispatcher]
    collect_status
    if arg[:update_period]
      Thread.new do
        EventMachine.run do
          EventMachine.add_periodic_timer(arg[:update_period]) do
            @logger.info "Periodically updating status"
            begin
              collect_status
            rescue => e
              @logger.error "Error collecting status"
              @logger.error e.message
              @logger.error e.backtrace.join("\n")
            end
            begin
              @logger.info 'Asked to reschedule'
              @dispatcher.reschedule
            rescue DRbConnError=> e
              @logger.error "Error contacting dispatcher for reschedule"
            end
          end
        end
      end
    end
    return
  end

  def collect_status(workers=@worker_server_table.keys)
    @logger.info "Collecting status"
    @lock.with_write_lock do
      workers.each do |w|
        status = nil
        avg_time = nil
        begin
          status = @worker_server_table[w].status
          avg_time = @worker_server_table[w].avg_running_time
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

    @logger.info "Successfully collected status"
    return
  end

  def register_job(job_id_list)
    @lock.with_write_lock do
      job_id_list.each do |job_id|
        @job_running_time[job_id] = []
        @logger.info "Job #{job_id} registered for recording"
      end
    end
    return
  end

  def delete_job(job_id_list)
    raise ArgumentError if job_id_list == nil
    job_id_list.is_a?Array or job_id_list = [job_id_list]
    @lock.with_write_lock do
      job_id_list.each do |job_id|
        @job_running_time.delete job_id
        @logger.info "Job #{job_id} removed from recording"
      end
    end
    return
  end

  def log_running_time(job_id, time)
    raise ArgumentError unless time.is_a? Float
    @lock.with_write_lock{@job_running_time[job_id] << time} # Necessary even it's a ReadWriteLockHash
    return 
  end

end

module StatusChecker::WorkerInterface
  def register_worker(worker)
    begin
      @worker_server_table[worker].status = Worker::STATUS::AVAILABLE
      @logger.info "Worker: #{worker} registered."
      @logger.info 'Asked to reschedule'
    rescue DRb::DRbConnError => e
      @logger.error "Error reaching the registering worker #{worker}"
      @logger.debug e.message
      @logger.debug e.backtrace.join("\n")
      raise "Can't reach worker, is your DRb service running?"
    end
    begin
      @dispatcher.reschedule
      @dispatcher.on_worker_available(worker)
    rescue DRbConnError => e
      @logger.error "Error reaching dispatcher"
      @logger.debug e.message
      @logger.debug e.backtrace.join("\n")
    end
    return
  end

  def mark_worker_status(worker, status)
    raise ArgumentError if !Worker::STATUS::constants.include? status
    @lock.with_write_lock do
      @worker_status_table[worker] = status
      @logger.info "Mark worker #{worker} as #{status}"
    end
    return
  end

  def on_task_done(worker, task_id, job_id, client_id)
    release_worker(worker)
    return
  end
  private :on_task_done
end
