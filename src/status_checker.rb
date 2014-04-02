require 'drb'

require_relative 'base_server'
require_relative 'worker'
require_relative 'common/read_write_lock'

class StatusChecker < BaseServer
  def worker_status
    @lock.with_read_lock{return @worker_status_table.clone}
  end
  def worker_uri(worker)
    return @worker_table[worker].instance_variable_get("@uri")
  end
  def initialize(worker_table={},arg={})
    super arg[:logger]
    # TODO: make up a worker table
    @lock = ReadWriteLock.new
    @worker_table = worker_table.clone
    @worker_status_table = Hash[worker_table.map{|w_id, w| [w_id, Worker::STATUS::UNKNOWN]}]
    @dispatcher = arg[:dispatcher]
    collect_status
  end
  def release_worker(worker)
    @lock.with_write_lock {
      @worker_status_table[worker] = Worker::STATUS::AVAILABLE
      @worker_table[worker].status = Worker::STATUS::AVAILABLE
      @logger.info "Released worker: #{worker}"
    }
    @dispatcher.on_worker_available(worker)
  end
  def occupy_worker(worker)
    @lock.with_write_lock {
      @worker_status_table[worker] = Worker::STATUS::OCCUPIED
      @worker_table[worker].status = Worker::STATUS::OCCUPIED
      @logger.info "Occupied worker: #{worker}"
    }
  end
  def worker_running(worker)
    @lock.with_write_lock {
      @logger.debug @worker
      @logger.debug @worker_status_table
      @logger.debug @worker_table
      @worker_status_table[worker] = Worker::STATUS::BUSY
      @worker_table[worker].status = Worker::STATUS::BUSY
      @logger.info "Mark running worker: #{worker}"
    }
  end
  def collect_status(workers=@worker_table.keys)
    @logger.info "Collecting status"
    @lock.with_write_lock do
      workers.each do |w|
        status = nil
        begin
          status = @worker_table[w].status
        rescue => e
          @logger.warn "Exception #{e} when checking status of worker #{w}"
          @logger.debug e.backtrace
          status = Worker::STATUS::DOWN
        ensure
          @logger.info "#{w} is #{status}"
          @worker_status_table[w] = status 
        end
      end
    end
  end
  # TODO: worker registration at runtime?
end
