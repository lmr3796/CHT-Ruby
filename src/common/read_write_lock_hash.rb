require_relative 'read_write_lock'

class ReadWriteLockHash < Hash
  def initialize(*args)
    super(*args)
    @read_write_lock = ReadWriteLock.new
  end
  def has_key?(*args)
    @read_write_lock.with_read_lock{return super(*args)}
  end
  def [](*args)
    @read_write_lock.with_read_lock{return super(*args)}
  end
  def []=(*args)
    @read_write_lock.with_write_lock{return super(*args)}
  end
  def delete(*args)
    @read_write_lock.with_write_lock{return super(*args)}
  end
  def delete_if(*args)
    @read_write_lock.with_write_lock{return super(*args)}
  end
  def keys(*args)
    @read_write_lock.with_read_lock{return super(*args)}
  end
  def merge(*args)
    @read_write_lock.with_read_lock{return super(*args)}
  end
  def merge!(*args)
    @read_write_lock.with_write_lock{return super(*args)}
  end
  def marshal_dump
    @read_write_lock.with_read_lock{return Hash.new.merge(self)}
  end
  def marshal_load(arg)
    update(arg)
    @read_write_lock = ReadWriteLock.new
  end
  def see_lock
    return @read_write_lock.inspect
  end
end
