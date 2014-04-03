require_relative 'read_write_lock'

class ReadWriteLockHash < Hash
  def initialize(*args)
    super(*args)
    @read_write_lock = ReadWriteLock.new
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
end
