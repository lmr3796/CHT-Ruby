require_relative 'read_write_lock'

class ReadWriteLockHash

  def initialize()
    @underlying_hash = {}
    @read_write_lock = ReadWriteLock.new
  end

  def [](key)
    value = nil
    @read_write_lock.with_read_lock{
      value = @underlying_hash[key]
    }
    return value
  end

  def []=(key, value)
    @read_write_lock.with_write_lock{
      @underlying_hash[key] = value
    }
  end
end
