require 'atomic'
require 'open3'

class Job
  attr_reader :task
  attr_accessor :priority, :deadline, :task_running_time_on_worker, :avg_task_running_time

  def initialize(priority=0, deadline=0, task_running_time_on_worker={})
    @task = []
    # Priority represented by smaller number is of higher priority
    @priority = priority
    @deadline = deadline
    @task_running_time_on_worker = task_running_time_on_worker
    @task_remaining = Atomic.new(0)
    return
  end

  def add_task(t)
    t.id = @task.size
    @task_remaining.update do |value|
      @task << t
      value + 1
    end
    return
  end

  def clear_task()
    @task_remaining.update do |value|
      @task = []
      0
    end
    return
  end

  def task_redo
    @task_remaining.update {|value| value + 1}
    return
  end

  def task_sent
    @task_remaining.update {|value| value - 1}
    return
  end

  def task_remaining
    return @task_remaining.value
  end

  def deadline=(deadline)
    raise ArgumentError unless deadline.is_a? Time
    return @deadline = deadline
  end



  def marshal_dump()
    [@task, @priority, @deadline, @task_running_time_on_worker, @task_remaining.value]
  end

  def marshal_load(array)
    @task, @priority, @deadline, @task_running_time_on_worker, @task_remaining = array
    @deadline = Time.at(deadline)
    @task_remaining = Atomic.new(@task_remaining)
  end

  def eql?(rhs)
    return false unless rhs.is_a? Job
    return marshal_dump().eql?(rhs.marshal_dump())
  end
end

class Task
  attr_accessor :id, :job_id
  attr_reader :cmd, :args

  def initialize(cmd, args=nil)
    @cmd = cmd
    @args = args
    return
  end

  def run()
    start = Time.now
    # Should use wait_thr instead of $?; $? not working when using DRb
    cmd_stdin, cmd_stdout, cmd_stderr, wait_thr = Open3.popen3(@cmd, *@args)  #TODO: Possible with a chroot?
    stdout = cmd_stdout.readlines.join('')
    stderr = cmd_stderr.readlines.join('')
    status = wait_thr.value
    cmd_stdin.close
    cmd_stdout.close
    cmd_stderr.close
    run_time = Time.now - start
    return TaskResult.new(@id, @job_id, run_time, status, stdout, stderr)
  rescue Interrupt, SystemExit
    wait_thr.kill
    wait_thr.join
    raise
  end

  def marshal_dump()
    [@id, @job_id, @cmd, @args]
  end

  def marshal_load(array)
    @id, @job_id, @cmd, @args = array
  end

  def eql?(rhs)
    return false unless rhs.is_a? Task
    return marshal_dump().eql?(rhs.marshal_dump())
  end
end

class TaskResult
  attr_reader :task_id, :job_id, :status, :stdout, :stderr
  attr_accessor :run_time
  def initialize(task_id, job_id, run_time, status, stdout, stderr)
    @task_id = task_id
    @job_id = job_id
    @run_time = run_time
    @status = status
    @stdout = stdout
    @stderr = stderr
    return
  end
end

class SleepTask < Task
  attr_reader :sleep_time
  def self.get_synthesized_success_status
    _1, _2, _3, wait_thr = Open3.popen3('sleep 0')
    return wait_thr.value
  end
  private_class_method :get_synthesized_success_status

  SUCCESS_STATUS = get_synthesized_success_status

  def initialize(sleep_time)
    raise ArugmentError unless sleep_time.is_a? Numeric
    @sleep_time = sleep_time
    return
  end

  def cmd()
    return "sleep"
  end

  def args()
    return [@sleep_time]
  end

  def run()
    sleep @sleep_time
    return TaskResult.new(@id, @job_id, @sleep_time, SUCCESS_STATUS, '', '') # Synthesized one
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
