require_relative 'rwlock'

class ReadWriteLockHash
  attr_accessor :rwlock

  def initialize(rwlock=ReadWriteLock.new, *args)
    @hash = Hash.new(*args)
    return @rwlock = ReadWriteLock.new
  end

  def replace(*args)
    @rwlock.with_write_lock{@hash.replace(*args)}
    return self
  end

  def has_key?(*args)
    return @rwlock.with_read_lock{@hash.has_key?(*args)}
  end

  def [](*args)
    return @rwlock.with_read_lock{@hash.[](*args)}
  end

  def []=(*args)
    return @rwlock.with_write_lock{@hash.[]=(*args)}
  end

  def delete(*args)
    return @rwlock.with_write_lock{@hash.delete(*args)}
  end

  def delete_if(*args)
    @rwlock.with_write_lock{@hash.delete_if(*args)}
    return self
  end

  def keys(*args)
    return @rwlock.with_read_lock{@hash.keys(*args)}
  end

  def has_key?(*args)
    return @rwlock.with_read_lock{@hash.has_key?(*args)}
  end

  def has_value?(*args)
    return @rwlock.with_read_lock{@hash.has_value?(*args)}
  end

  def merge(*args)
    return @rwlock.with_read_lock{@hash.merge(*args)}
  end

  def merge!(*args)
    @rwlock.with_write_lock{@hash.merge!(*args)}
    return self
  end
  
  def hash_clone(*args)
    return @rwlock.with_read_lock{@hash.clone}
  end

  def clone()
    return @rwlock.with_read_lock do
      c = ReadWriteLockHash.new
      c.replace(@hash)
    end
  end

  def rwlock=(rwlock)
    rwlock.is_a? ReadWriteLock or raise ArgumentError
    @rwlock = rwlock
    return rwlock
  end
end
