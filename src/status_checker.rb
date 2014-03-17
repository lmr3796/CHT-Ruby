require 'drb'

require_relative 'common/read_write_lock_hash'
require_relative 'worker'

class StatusChecker
  def initialize(worker_table={})
    # TODO: make up a worker table
    @worker_table = worker_table.clone
    @worker_status_table = Hash[worker_table.map{|w_id, w| [w_id, Worker::STATUS::UNKNOWN]}]
  end
  def release_worker(worker)
    @worker_status_table[worker] = Worker::STATUS::AVAILABLE
  end
  def occupy_worker(worker)
    @worker_status_table[worker] = Worker::STATUS::OCCUPIED
  end
  def worker_running(worker)
    @worker_status_table[worker] = Worker::STATUS::BUSY
  end
  def collect_status(worker=nil)
    # TODO: collect status of all/specified worker
    raise NotImplementedError
  end
  # TODO: worker registration at runtime?
end
