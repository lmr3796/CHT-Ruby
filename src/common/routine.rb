module Routine
  def Routine.every_n_seconds(n)
    loop do
      before = Time.now
      yield
      interval = n - (Time.now - before)
      sleep(interval) if interval > 0
    end
  end
end
