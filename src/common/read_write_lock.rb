require 'thread'

class ReadWriteLock
  def initialize()
    @mutex = Mutex.new
    @rwlock = Mutex.new
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
    @mutex.synchronize do
      @rwlock.lock if @read_count == 0
      @read_count += 1
    end
  end

  def release_read_lock()
    @mutex.synchronize do
      raise "@read_count corrupted" if @read_count <= 0
      @read_count -= 1
      @rwlock.unlock if @read_count == 0
    end
  end

  def require_write_lock()
    @rwlock.lock
  end

  def release_write_lock()
    @rwlock.unlock
  end

  private :require_read_lock, :require_write_lock,
    :release_read_lock, :release_write_lock
end
