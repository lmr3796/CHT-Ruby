require 'open3'
require 'logger/colors'
require 'time'
require 'thread'
require 'securerandom'

require_relative 'base_server'
require_relative 'job.rb'

class Worker < BaseServer
  attr_reader :name, :status, :id, :avg_running_time
  attr_writer :status_checker

  LEARNING_RATE = 0.2

  module STATUS
    DOWN       = :DOWN
    UNKNOWN    = :UNKNOWN
    OCCUPIED   = :OCCUPIED    # Assigned to a job but not running a task
    AVAILABLE  = :AVAILABLE   # Idle, not assigned to a job
    BUSY       = :BUSY        # Running a task
  end

  def initialize(name, arg={})
    super arg[:logger]
    @id = SecureRandom::uuid()
    @name = name
    @lock = Mutex.new
    @status = STATUS::AVAILABLE
    @avg_running_time = nil
  end

  def register()
    @logger.info "Notifies status checker for coming"
    @status_checker.register_worker @name
  end

  def status=(s)
    raise ArgumentError if !STATUS::constants.include? s
    @logger.info "Worker status set to #{s}"
    @status = s
  end

  def log_running_time(job_id, time)
    @logger.info "Logs runtime to self"
    @avg_running_time = @avg_running_time == nil ? time : @avg_running_time * (1-LEARNING_RATE) + time * LEARNING_RATE
    @logger.info "Logs runtime to status_checker"
    @status_checker.log_running_time job_id, time
  end

  def release()
    # TODO: what if maliciously called?
    @status_checker.release_worker @name
  end

  def run_task(task, job_uuid=nil)
    if !task.is_a? Task
      @logger.info "The input is not a task"
      raise 'Not a proper task to run'
    end
    res = nil
    @lock.synchronize{  # Worker is dedicated
      @status_checker.worker_running @name
      @logger.info "Running task of job #{job_uuid}"
      res, elapsed= run_cmd(task.cmd, *task.args)
      @logger.info "Finished task of job #{job_uuid} in #{elapsed} seconds"
    }
    return res
  end

  def run_cmd(command, *args)
    @logger.debug "Running `#{command}#{args.join(' ')}`"
    start = Time.now
    # Should use wait_thr instead of $?; $? not working when using DRb
    stdin, stdout, stderr, wait_thr = Open3.popen3(command, *args)  #TODO: Possible with a chroot?
    result = {
      :stdout => stdout.readlines.join(''),
      :stderr => stderr.readlines.join(''),
      :exit_status => wait_thr.value
    }
    stdin.close
    stdout.close
    stderr.close
    elapsed = Time.now - start
    result[:elapsed] = elapsed
    @logger.debug "`#{command}#{args.join(' ')}` done in #{elapsed} seconds"
    return result, elapsed
  end

end

