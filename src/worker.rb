require 'open3'
require 'thread'
require 'securerandom'

require_relative 'job.rb'

class Worker
  attr_reader :name, :status, :id
  attr_writer :status_checker


  module STATUS
    DOWN       = :DOWN
    UNKNOWN    = :UNKNOWN
    OCCUPIED   = :OCCUPIED
    AVAILABLE  = :AVAILABLE
    BUSY       = :BUSY
  end

  def initialize(name, logger=Logger.new(STDERR))
    @id = SecureRandom::uuid()
    @name = name
    @lock = Mutex.new
    @status = STATUS::AVAILABLE
  end

  def run_task(task, job_uuid=nil)
    raise 'Not a proper task to run' if !task.is_a? Task
    res = nil
    @lock.synchronize{  # Worker is dedicated
      @status = STATUS::BUSY
      @status_worker.worker_running @id
      res = run_cmd(task.cmd, task.args)
      @status = STATUS::AVAILABLE
      @status_worker.release_worker @id
    }
    return res
  end

  def run_cmd(command, *args)
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

