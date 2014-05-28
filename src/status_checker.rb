require 'drb'
require 'eventmachine'

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
    if arg[:update_period]
      Thread.new do
        EventMachine.run do
          EventMachine.add_periodic_timer(arg[:update_period]) do
            collect_status
            @logger.info 'Asked to reschedule'
            begin
              @dispatcher.reschedule
            rescue
              @logger.error "Error reaching dispatcher"
            end
          end
        end
      end
    end
  end
  def register_worker(worker)
    @lock.with_write_lock {
      @worker_status_table[worker] = @worker_table[worker].status = Worker::STATUS::AVAILABLE
      @logger.info "Worker: #{worker} registered; set to AVAILABLE"
    }
    @logger.info 'Asked to reschedule'
    begin
      @dispatcher.reschedule
    rescue
      @logger.error "Error reaching dispatcher"
    end
    @dispatcher.on_worker_available(worker)
  end
  def release_worker(worker)
    @lock.with_write_lock {
      @worker_status_table[worker] = @worker_table[worker].status = Worker::STATUS::AVAILABLE
      @logger.info "Released worker: #{worker}"
    }
    @dispatcher.on_worker_available(worker)
  end
  def occupy_worker(worker)
    @lock.with_write_lock {
      @worker_status_table[worker] = @worker_table[worker].status = Worker::STATUS::OCCUPIED
      @logger.info "Occupied worker: #{worker}"
    }
  end
  def worker_running(worker)
    @lock.with_write_lock {
      @worker_status_table[worker] = @worker_table[worker].status = Worker::STATUS::BUSY
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
          @logger.debug e.to_s
          status = Worker::STATUS::DOWN
        ensure
          @logger.info "#{w} is #{status}"
          @worker_status_table[w] = status 
        end
      end
    end
    workers.select{|w| @worker_status_table[w] == Worker::STATUS::AVAILABLE}.each do |w|
      begin
        @dispatcher.on_worker_available w
      rescue
        @logger.error "Error reaching dispatcher"
      end
    end
  end
  # TODO: worker registration at runtime?
end
