require_relative 'read_write_lock'

class ReadWriteLockHash < Hash
  def initialize(*args)
    super(*args)
    return @read_write_lock = ReadWriteLock.new
  end
  def has_key?(*args)
    return @read_write_lock.with_read_lock{super(*args)}
  end
  def [](*args)
    return @read_write_lock.with_read_lock{super(*args)}
  end
  def []=(*args)
    return @read_write_lock.with_write_lock{super(*args)}
  end
  def delete(*args)
    return @read_write_lock.with_write_lock{super(*args)}
  end
  def delete_if(*args)
    return @read_write_lock.with_write_lock{super(*args)}
  end
  def keys(*args)
    return @read_write_lock.with_read_lock{super(*args)}
  end
  def merge(*args)
    return @read_write_lock.with_read_lock{super(*args)}
  end
  def merge!(*args)
    return @read_write_lock.with_write_lock{super(*args)}
  end
  #def marshal_dump
  #  @read_write_lock.with_read_lock{Hash.new.merge(self)}
  #end
  #def marshal_load(arg)
  #  @read_write_lock = ReadWriteLock.new
  #  update(arg)
  #end
  #def see_lock
  #  return @read_write_lock.inspect
  #end
end
