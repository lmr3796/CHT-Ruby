
class Job
  attr_accessor :priority, :deadline
  def initialize(priority, deadline)
    @task = []
    @priority = priority
    @deadline = deadline
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
