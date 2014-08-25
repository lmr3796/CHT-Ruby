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
    @progress = Atomic.new(Progress.new)
    return
  end

  # Should be invoked only on Task generation
  def add_task(t)
    @progress.update do |progress|
      t.id = @task.size
      @task << t
      progress.add_task
    end
    return
  end

  def task_redo(task_id)
    @progress.update do |progress|
      progress.task_redo(task_id)
    end
    return
  end

  def task_sent(task_id)
    @progress.update do |progress|
      progress.task_sent(task_id)
    end
    return
  end

  def task_done(task_id)
    @progress.update do |progress|
      progress.task_done(task_id)
    end
    return
  end

  def progress
    return @progress.value.clone
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
    @progress = Atomic.new(Job::Progress.new(@progress))
  end

  def eql?(rhs)
    return false unless rhs.is_a? Job
    return marshal_dump().eql?(rhs.marshal_dump())
  end
end

class Job::Progress
  attr_reader :queued, :sent, :done
  class InconsistentUpdateError < RuntimeError;end
  module JobState
    QUEUED=:QUEUED
    SENT=:SENT
    DONE=:DONE
  end

  def clone
    return self.class.new(self)
  end

  def initialize(job_status=[])
    job_status = job_status.job_status if job_status.is_a?(self.class)
    raise ArgumentError unless job_status.is_a?(Array)
    @job_status = job_status.clone
    return
  end

  def [](task_id)
    return @job_status[task_id]
  end

  def job_status
    return @job_status.clone
  end

  def add_task
    @job_status << JobState::QUEUED
    return self
  end

  def task_sent(task_id)
    @job_status[task_id] = JobState::SENT
    return self
  end

  def task_done(task_id)
    @job_status[task_id] = JobState::DONE
    return self
  end

  def task_redo(task_id)
    @job_status[task_id] = JobState::QUEUED
    return self
  end

  def total
    return @job_status.size
  end

  def queued
    return @job_status.each_with_index.select{|s, i| s == JobState::QUEUED}.map{|s,i| i}
  end

  def sent
    return @job_status.each_with_index.select{|s, i| s == JobState::SENT}.map{|s,i| i}
  end

  def done
    return @job_status.each_with_index.select{|s, i| s == JobState::DONE}.map{|s,i| i}
  end

  def undone
    return @job_status.each_with_index.select{|s, i| s != JobState::DONE}.map{|s,i| i}
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
  attr_accessor :sleep_time
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

  def sleep_time=(t)
    raise ArugmentError unless t.is_a? Numeric
    @sleep_time = t
  end
end

class GPUSleepTask < SleepTask
  def initialize(*args)
    super
  end

  def to_sleep_task(factor)
    raise ArgumentError if !factor.is_a? Numeric
    new_task = Marshal.load(Marshal.dump(self))
    new_task.sleep_time = @sleep_time * factor
    return new_task
  end
end
