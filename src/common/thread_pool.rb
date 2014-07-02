require 'thread'
require 'securerandom'
require_relative 'rwlock_hash'

class ThreadPool

  class Job
    def initialize(id, *args, &block)
      @id = id
      @args = args
      @block = block
      @mutex = Mutex.new
      @cv = ConditionVariable.new
      @done = nil
    end

    def done!()
      @mutex.synchronize{
        @done = true
        @cv.signal
      }
    end

    def wait()
      @mutex.synchronize{
        @cv.wait(@mutex) unless @done
      }
    end

    attr_accessor :id, :args, :block
  end

  def initialize(size)
    @size = size
    @job_queue = Queue.new
    @jobs = ReadWriteLockHash.new
    @workers = Array.new(@size){ |i|
      Thread.new{
        Thread.current[:id] = i
        worker_routine
      }
    }
  end

  def schedule(*args, &block)
    job_id = SecureRandom.uuid
    @jobs[job_id] = Job.new(job_id, *args, &block)
    @job_queue.push(job_id)
    return job_id
  end

  def join(job_id)
    job = @jobs[job_id]
    raise "Job does not exist." unless job.is_a? Job
    job.wait
    @jobs.delete(job_id)
  end

  def shutdown()
    @workers.each{ |worker|
      worker.kill
    }
  end

  def worker_routine()
    while true
      job_id = @job_queue.pop
      job = @jobs[job_id]
      begin
        job.block.call(*(job.args))
      rescue Exception => e
        message = "Exception raised in a block scheduled by ThreadPool: #{e.message} (#{e.class.to_s})\n"
        $stderr.puts message
        $stderr.puts e.backtrace
      end
      job.done!
    end
  end
  private :worker_routine

end
