require 'thread'
require 'securerandom'
require_relative 'read_write_lock_hash'

class ThreadPool
  def initialize(size)
    @size = size
    @job_queue = Queue.new
    @job_block = ReadWriteLockHash.new
    @job_mutex = ReadWriteLockHash.new
    @job_state = ReadWriteLockHash.new
    @workers = Array.new(@size){ |i|
      Thread.new{
        worker_routine
      }
    }
  end

  def schedule(*args, &block)
    job_id = SecureRandom.uuid
    @job_block[job_id] = [args, block]
    @job_queue.push(job_id)
    @job_mutex[job_id] = [Mutex.new, ConditionVariable.new]
    return job_id
  end

  def join(job_id)
    @job_mutex[job_id][0].synchronize{
      @job_mutex[job_id][1].wait(@job_mutex[job_id][0]) if @job_state[job_id] != :END
    }
    @job_state.delete(job_id)
    @job_mutex.delete(job_id)
  end

  def shutdown()
    @workers.each{ |worker|
      worker.kill
    }
  end

  def worker_routine()
    while true
      job_id = @job_queue.pop
      @job_mutex[job_id][0].synchronize{
        @job_state[job_id] = :RUNNING
      }
      args, block = @job_block[job_id]
      @job_block.delete(job_id)
      block.call(*args)
      @job_mutex[job_id][0].synchronize{
        @job_state[job_id] = :END
        @job_mutex[job_id][1].signal
      }
    end
  end

  private :worker_routine
end
