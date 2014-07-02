require_relative 'rwlock'

class ReadWriteLockHash < Hash
  attr_accessor :rwlock
  def initialize(rwlock=ReadWriteLock.new, *args)
    super(*args)
    return @rwlock = ReadWriteLock.new
  end
  def has_key?(*args)
    return @rwlock.with_read_lock{super(*args)}
  end
  def [](*args)
    return @rwlock.with_read_lock{super(*args)}
  end
  def []=(*args)
    return @rwlock.with_write_lock{super(*args)}
  end
  def delete(*args)
    return @rwlock.with_write_lock{super(*args)}
  end
  def delete_if(*args)
    return @rwlock.with_write_lock{super(*args)}
  end
  def keys(*args)
    return @rwlock.with_read_lock{super(*args)}
  end
  def merge(*args)
    return @rwlock.with_read_lock{super(*args)}
  end
  def merge!(*args)
    return @rwlock.with_write_lock{super(*args)}
  end
  def rwlock=(rwlock)
    rwlock.is_a? ReadWriteLock or raise ArgumentError
    @rwlock = rwlock
  end
  #def marshal_dump
  #  @rwlock.with_read_lock{Hash.new.merge(self)}
  #end
  #def marshal_load(arg)
  #  @rwlock = ReadWriteLock.new
  #  update(arg)
  #end
  #def see_lock
  #  return @rwlock.inspect
  #end
end
