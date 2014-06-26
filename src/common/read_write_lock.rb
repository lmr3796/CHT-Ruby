require 'thread'

class ReadWriteLock
  def initialize()
    @mutex = Mutex.new
    @write = Mutex.new
    @read_count = 0
  end

  def with_read_lock()
    require_read_lock
    result = yield
  ensure
    release_read_lock
    return result
  end

  def with_write_lock()
    require_write_lock
    result = yield
  ensure
    release_write_lock
    return result
  end

  def require_read_lock()
    @mutex.synchronize do
      @write.lock if @read_count == 0
      @read_count += 1
    end
  end

  def release_read_lock()
    @mutex.synchronize do
      @read_count -= 1
      @write.unlock if @read_count == 0 && @write.owned?
    end
  end

  def require_write_lock()
    @write.lock unless @write.owned?
  end

  def release_write_lock()
    @write.unlock
  end
  private :require_read_lock, :require_write_lock,
    :release_read_lock, :release_write_lock
end
