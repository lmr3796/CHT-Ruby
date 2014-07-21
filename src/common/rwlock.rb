require 'thread'

class ReadWriteLock
  def initialize()
    @mutex = Mutex.new
    @cv = ConditionVariable.new
    @read_count = 0
    @write_count = 0
    @write_waiting = 0
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
      while @write_count > 0 || @write_waiting > 0
        @cv.wait @mutex
      end
      @read_count += 1
    end
    return
  end

  def release_read_lock()
    @mutex.synchronize do
      raise "@read_count corrupted" if @read_count <= 0
      @read_count -= 1
      @cv.signal if @read_count == 0
    end
    return
  end

  def require_write_lock()
    @mutex.synchronize do
      @write_waiting += 1
      while @read_count > 0 || @write_count > 0
        @cv.wait @mutex
      end
      @write_waiting -= 1
      @write_count += 1
    end
  end

  def release_write_lock()
    @mutex.synchronize do
      raise "@write_count corrupted" if @write_count != 1
      @write_count -= 1
      @cv.signal
    end
  end

  private :require_read_lock, :require_write_lock,
    :release_read_lock, :release_write_lock
end
