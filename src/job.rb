
class Job
  attr_reader :task
  attr_accessor :priority, :deadline, :task_running_time_on_worker, :avg_task_running_time
  def initialize(priority=0, deadline=0, task_running_time_on_worker={})
    @task = []
    # Priority represented by smaller number is of higher priority
    @priority = priority
    @deadline = deadline
    @task_running_time_on_worker = task_running_time_on_worker
  end
  def add_task(t)
    @task << t
  end
  def clear_task()
    @task = []
  end

  def deadline=(deadline)
    raise ArgumentError unless deadline.is_a? Time
    @deadline = deadline
  end

  def marshal_dump()
    [@task, @priority, @deadline, @task_running_time_on_worker]
  end

  def marshal_load(array)
    @task, @priority, @deadline, @task_running_time_on_worker = array
    @deadline = Time.at(deadline)
  end

end

class Task
  attr_reader :cmd, :args
  def initialize(cmd, args=nil)
    @cmd = cmd
    @args = args
  end
end
