require 'drb'

require_relative 'common/read_write_lock_hash'
require_relative 'worker'

class StatusChecker
  def initialize(worker_status_table={})
    @worker_status_table = ReadWriteLockHash.new worker_status_table
  end
  def release_worker(worker)
    @worker_status_table[worker] = Worker::STATE[:AVAILABLE]
  end
  def occupy_worker(worker)
    @worker_status_table[worker] = Worker::STATE[:OCCUPIED]
  end
  def worker_running(worker)
    @worker_status_table[worker] = Worker::STATE[:BUSY]
  end
  def add_worker(worker, state=Worker::STATE[:AVAILABLE])
    raise ArgumentError.new('Invalid state') if !Worker::STATE.has_value? state
    raise ArgumentError.new('Worker exists') if @worker_status_table.include? worker
    @worker_status_table[worker] = state
  end
end
