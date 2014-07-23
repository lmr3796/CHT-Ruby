require 'thread'

class ReadWriteLock
  def initialize()
    @mutex = Mutex.new
    @cv = ConditionVariable.new
    @reading = 0
    @writing = 0
    @writer_waiting = 0
    @writer_first = false
    return
  end

  def with_read_lock()
    require_read_lock
    yield
  ensure
    release_read_lock
  end

  def with_write_lock()
    require_write_lock
    yield
  ensure
    release_write_lock
  end

  def require_read_lock()
    @mutex.synchronize do
      while @writing > 0 || (@writer_waiting > 0 && @writer_first)
        @cv.wait @mutex
      end
      @reading += 1
    end
    return
  end

  def release_read_lock()
    @mutex.synchronize do
      raise "@reading corrupted" if @reading <= 0
      @reading -= 1
      @writer_first = true
      @cv.broadcast if @reading == 0
    end
    return
  end

  def require_write_lock()
    @mutex.synchronize do
      @writer_waiting += 1
      while @reading > 0 || @writing > 0
        @cv.wait @mutex
      end
      @writer_waiting -= 1
      @writing += 1
    end
  end

  def release_write_lock()
    @mutex.synchronize do
      raise "@writing corrupted" if @writing != 1
      @writing -= 1
      @writer_first = false
      @cv.broadcast
    end
  end

  private :require_read_lock, :require_write_lock,
    :release_read_lock, :release_write_lock
end
