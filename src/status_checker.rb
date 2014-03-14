require 'drb'

require_relative 'common/read_write_lock_hash'
require_relative 'worker'

class StatusChecker
  def initialize(worker_table={})
    # TODO: make up a worker table
    @worker_table = worker_table.clone
    @worker_status_table = Hash[worker_table.map{|w_id, w| [w_id, Worker::STATE::UNKNOWN]}]
  end
  def release_worker(worker)
    @worker_status_table[worker] = Worker::STATE::AVAILABLE
  end
  def occupy_worker(worker)
    @worker_status_table[worker] = Worker::STATE::OCCUPIED
  end
  def worker_running(worker)
    @worker_status_table[worker] = Worker::STATE::BUSY
  end
  def collect_state(worker=nil)
    # TODO: collect state of all/specified worker
  end
  # TODO: worker registration at runtime?
end
