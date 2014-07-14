require_relative 'worker'
require_relative 'job'

class SimulatedHeterogeneousWorker < Worker
  def initialize(name, args = {})
    super
    # TODO: recognize the arguments of the distribution of actual sleeping time
  end

  def run_task(task)
    @logger.fatal task.inspect and raise 'Invalid task to run' if !task.is_a? Task
    @logger.debug "#{task.job_id}[#{task.id}] running."
    @logger.info "Running `#{task.cmd} #{task.args.join(' ')}`"
    sleep_time_computer = Proc.new do |time|
      # TODO: compute the actual sleeping time on this worker
      time
    end
    result = task.run {:actual_sleep_time_computer=>sleep_time_computer}
    log_running_time(result.job_id, result.run_time)
    @logger.debug "Finished #{result.job_id}[#{result.task_id}] in #{result.run_time} seconds"
    return result
  end
end

class SleepTask < Task
  attr_accessor :id, :job_id

  def initialize(sleep_time)
    @sleep_time = sleep_time
    return
  end

  def cmd()
    return "sleep #{@sleep_time}"
  end

  def args()
    return nil
  end

  def run(args = {})
    if args.has_key? :actual_sleep_time_computer
      sleep_time = args[:actual_sleep_time_computer].call(@sleep_time)
    else
      sleep_time = @sleep_time
    end
    cmd = "sleep #{sleep_time}"

    start = Time.now
    # Should use wait_thr instead of $?; $? not working when using DRb
    cmd_stdin, cmd_stdout, cmd_stderr, wait_thr = Open3.popen3(cmd)  #TODO: Possible with a chroot?
    stdout = cmd_stdout.readlines.join('')
    stderr = cmd_stderr.readlines.join('')
    status = wait_thr.value
    cmd_stdin.close
    cmd_stdout.close
    cmd_stderr.close
    run_time = Time.now - start
    return TaskResult.new(@id, @job_id, run_time, status, stdout, stderr)
  end

  def marshal_dump()
    [@id, @job_id, @sleep_time]
  end

  def marshal_load(array)
    @id, @job_id, @sleep_time = array
  end

  def eql?(rhs)
    return false unless rhs.is_a? Task
    return marshal_dump().eql?(rhs.marshal_dump())
  end
end
