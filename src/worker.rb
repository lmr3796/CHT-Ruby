require 'open3'

require_relative 'job.rb'

class Worker
  attr_reader :name

  STATE={
    :OCCUPIED => :OCCUPIED,
    :AVAILABLE => :AVAILABLE,
    :BUSY => :BUSY,
  }

  def initialize(name)
    @name = name
  end

  def run_task(task, job_uuid=nil)
    raise 'Not a proper task to run' if !task.is_a? Task
    return run_cmd(task.cmd, task.args)
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

  def notify_pull_job() # Notified to pull jobs
  end

end

