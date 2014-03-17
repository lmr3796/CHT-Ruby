
class Job
  attr_accessor :priority, :deadline, :worker_running_time_table
  def initialize(priority, deadline, worker_running_time_table)
    @task = []
    # Priority represented by smaller number is of higher priority
    @priority = priority
    @deadline = deadline
    @worker_running_time_table = worker_running_time_table
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
