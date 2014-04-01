require 'open3'
require 'logger'
require 'thread'
require 'securerandom'

require_relative 'job.rb'

class Worker
  attr_reader :name, :status, :id
  attr_writer :status_checker

  module STATUS
    DOWN       = :DOWN
    UNKNOWN    = :UNKNOWN
    OCCUPIED   = :OCCUPIED    # Assigned to a job but not running a task
    AVAILABLE  = :AVAILABLE   # Idle, not assigned to a job
    BUSY       = :BUSY        # Running a task
  end

  def initialize(name, logger=Logger.new(STDERR))
    @id = SecureRandom::uuid()
    @name = name
    @lock = Mutex.new
    @status = STATUS::AVAILABLE
    @logger = logger
  end

  def status=(s)
    raise ArgumentError if !STATUS::constants.include? s
    @logger.info "Worker status set to #{s}"
    @status = s
  end

  def run_task(task, job_uuid=nil)
    raise 'Not a proper task to run' if !task.is_a? Task
    res = nil
    @lock.synchronize{  # Worker is dedicated
      @logger.info "Running task of #{job_uuid}"
      @status_checker.worker_running @name
      res = run_cmd(task.cmd, task.args)
      @status_checker.release_worker @name
      @logger.info "Finished task of #{job_uuid}"
    }
    return res
  end

  def run_cmd(command, *args)
    @logger.debug("Run `#{command} #{args.join(' ')}`")
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
    return result
  end

end

