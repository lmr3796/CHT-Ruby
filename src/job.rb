require 'atomic'

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

TaskResult = Struct.new(:task_id, :job_id, :status, :run_time, :stdout, :stderr)
class TaskResult
  def initialize(task_id, job_id, arg={})
    super(task_id, job_id, arg[:status], arg[:run_time], arg[:stdout], arg[:stderr])
    return
  end
end
