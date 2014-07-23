require 'atomic'
require 'open3'

class Job; end

class Job::Progress
  attr_reader :queued, :sent, :done
  def initialize(queued=0, sent=0, done=0)
    raise ArgumentError unless queued.is_a?(Integer) && queued >= 0
    raise ArgumentError unless sent.is_a?(Integer) && sent >= 0
    raise ArgumentError unless done.is_a?(Integer) && done >= 0
    @queued = queued
    @sent = sent
    @done = done
  end

  def total
    return @queued + @sent + @done
  end

  def undone
    return @queued + @sent
  end

  def mutate(args = {})
    default = Hash[:queued, 0, :sent, 0, :done, 0]
    args = default.merge(args)
    return Job::Progress.new(@queued + args[:queued], @sent + args[:sent], @done + args[:done])
  end
end

class Job
  attr_reader :task
  attr_accessor :priority, :deadline, :task_running_time_on_worker, :avg_task_running_time

  def initialize(priority=0, deadline=0, task_running_time_on_worker={})
    @task = []
    # Priority represented by smaller number is of higher priority
    @priority = priority
    @deadline = deadline
    @task_running_time_on_worker = task_running_time_on_worker
    @progress = Atomic.new(Progress.new)
    return
  end

  # Should be invoked only on Task generation
  def add_task(t)
    t.id = @task.size
    @progress.update do |progress|
      @task << t
      progress.mutate(:queued => +1)
    end
    return
  end

  def task_redo
    @progress.update do |progress|
      progress.mutate(:sent => -1, :queued => +1)
    end
    return
  end

  def task_sent
    @progress.update do |progress|
      raise 'No queued task' if progress.queued == 0
      progress.mutate(:queued => -1, :sent => +1)
    end
    return
  end

  def task_done
    @progress.update do |progress|
      raise 'No sent task to be done' if progress.sent == 0
      progress.mutate(:sent => -1, :done => +1)
    end
    return
  end

  def progress
    return @progress.value
  end

  def deadline=(deadline)
    raise ArgumentError unless deadline.is_a? Time
    return @deadline = deadline
  end



  def marshal_dump()
    [@task, @priority, @deadline, @task_running_time_on_worker, @progress.value]
  end

  def marshal_load(array)
    @task, @priority, @deadline, @task_running_time_on_worker, @progress = array
    @deadline = Time.at(deadline)
    @progress = Atomic.new(@progress)
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
