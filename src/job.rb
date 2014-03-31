
class Job
  attr_accessor :priority, :deadline, :task_running_time_on_worker
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
end

class Task
  attr_reader :cmd, :args
  def initialize(cmd, args=nil)
    @cmd = cmd
    @args = args
  end
end
