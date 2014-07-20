require 'drb'
require 'timers'

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

  def periodic_check()
    @logger.info "Periodically collecting status and rescheduling"
    collect_status
    @logger.info 'Asked to reschedule'
    @dispatcher.reschedule
    return
  rescue DRb::DRbConnError
    @logger.error "Error contacting dispatcher for reschedule"
  end
  private :periodic_check

  def initialize(worker_table={},arg={})
    super arg[:logger]
    # TODO: make up a worker table
    @lock = ReadWriteLock.new
    @job_running_time = ReadWriteLockHash.new
    @worker_server_table = Hash[worker_table]
    @worker_status_table = Hash[worker_table.map{|w_id, w| [w_id, Worker::STATUS::UNKNOWN]}]
    @worker_avg_running_time = Hash[worker_table.map{|w_id, w| [w_id, nil]}]
    @dispatcher = arg[:dispatcher]
    timer_group = Timers::Group.new
    @periodic_checker = timer_group.every(arg[:update_period] || 1){periodic_check}
    @update_period = arg[:update_period]
    raise ArgumentError if @update_period != nil && !@update_period.is_a?(Numeric)
    return

  end

  def register
    raise ArgumentError if @update_period != nil && !@update_period.is_a?(Numeric)
    Thread.new(timer_group){|t|loop{t.wait}} if @update_period != nil
    @periodic_checker.fire
    return
  end

  # The periodic interval resets if we're asked to recollect
  def require_recollect_status
    return @periodic_checker.fire
  end

  def collect_status()
    workers=@worker_server_table.keys
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

    release_zombie_occupied_worker
    awake_idle_worker
    @logger.info "Successfully collected status"
    return
  end
  private :collect_status

  def preempt_unmatch_jobs
    @worker_server_table.values.each{|w| w.preempt_if_unmatch}
  end

  def release_zombie_occupied_worker(workers=@worker_server_table.keys)
    # Free occupied workers
    workers.select{|w|@worker_status_table[w] == Worker::STATUS::OCCUPIED}.each do |w|
      @logger.debug "Worker #{w} is occupied; ask to validate"
      valid = @worker_server_table[w].validate_occupied_assignment
      @logger.debug "Worker #{w} assignment valid: #{valid}"
    end
    @logger.info "Released workers occupied by zombie jobs"
  end

  def awake_idle_worker(workers=@worker_server_table.keys)
    # Force all idle available workers to pull for possible jobs
    worker_failure = false
    workers.select{|w| @worker_status_table[w] == Worker::STATUS::AVAILABLE}.each do |w|
      begin
        @worker_server_table[w].awake
      rescue DRb::DRbConnError => e
        @logger.error "Error reaching worker #{w}"
        @logger.error e.backtrace.join("\n")
        @worker_status_table[w] = Worker::STATUS::DOWN
      end
    end
    @logger.info "Awoke idle available workers"
    @logger.warn "Some workers are down, need reschedule" and reschedule if worker_failure
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

  def delete_job_from_logging(job_id_list)
    raise ArgumentError if job_id_list == nil
    job_id_list.is_a?Array or job_id_list = [job_id_list]
    @lock.with_write_lock do
      job_id_list.each do |job_id|
        @job_running_time.delete job_id
        @logger.info "Job #{job_id} removed from logging"
      end
    end
    return
  end

  def log_running_time(job_id, time)
    raise ArgumentError unless time.is_a? Float
    @lock.with_write_lock do # Necessary even it's a ReadWriteLockHash
      @logger.debug "Logging runtime of #{job_id}"
      @job_running_time[job_id] << time
      @logger.debug "Logged runtime of #{job_id}"
    end
    return
  end

end

module StatusChecker::WorkerInterface
  def register_worker(worker)
    begin
      mark_worker_status(worker, Worker::STATUS::AVAILABLE)
      @logger.info "Worker: #{worker} registered."
      begin
        @logger.info 'Asked to reschedule'
        @dispatcher.reschedule
      rescue DRb::DRbConnError => e
        @logger.error "Error reaching dispatcher for rescheduling"
        @logger.error e.message
        @logger.error e.backtrace.join("\n")
      end
      @worker_server_table[worker].status = Worker::STATUS::AVAILABLE
    rescue DRb::DRbConnError => e
      @logger.error "Can't reach worker, is your DRb service running?"
      @logger.error "Error reaching the registering worker #{worker}"
      @logger.error e.message
      @logger.error e.backtrace.join("\n")
    end
    return
  end

  def mark_worker_status(worker, status)
    raise ArgumentError if !Worker::STATUS::constants.include? status
    @lock.with_write_lock do
      @worker_status_table[worker] = status
      @logger.debug "Mark worker #{worker} as #{status}"
    end
    return
  end

  def on_task_done(worker, task_id, job_id, client_id)
    release_worker(worker)
    return
  end
  private :on_task_done
end
