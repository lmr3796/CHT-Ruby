require_relative 'read_write_lock'

class ReadWriteLockHash

  def initialize(arg={})
    @underlying_hash = arg.clone
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

  def delete(key)
    @read_write_lock.with_write_lock{
      @underlying_hash.delete(key)
    }
  end

  def keys()
    return @underlying_hash.keys
  end

  def merge(hash, &block)
    res = nil
    @read_write_lock.with_read_lock{
      res = @underlying_hash.merge(hash, &block)
    }
    return res
  end

  def merge!(hash, &block)
    @read_write_lock.with_write_lock{
      @underlying_hash.merge!(hash, &block)
    }
  end

end
