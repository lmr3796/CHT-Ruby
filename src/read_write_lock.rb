require 'thread'

class ReadWriteLock
  def initialize()
    @mutex = Mutex.new
    @write = Mutex.new
    @read_count = 0
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
    @mutex.lock()
    @read_count = @read_count + 1
    @write.lock if @read_count == 1
    @mutex.unlock()
  end

  def release_read_lock()
    @mutex.lock()
    @read_count = @read_count - 1
    @write.unlock if @read_count == 0
    @mutex.unlock()
  end

  def require_write_lock()
    @write.lock
  end

  def release_write_lock()
    @write.unlock
  end
end
