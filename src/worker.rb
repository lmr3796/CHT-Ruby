require 'open3'
require 'logger/colors'
require 'time'
require 'thread'
require 'securerandom'

require_relative 'base_server'
require_relative 'job.rb'
require_relative 'common/read_write_lock'

class TaskResult
  def initialize()
  end
end
class TaskResultManager
  def initialize()
    @lock = ReadWriteLock.new
    @task_result = Hash.new {|h,k| h[k] = []}
    @task_result_by_client = Hash.new {|h,k| h[k] = []} # Contains job_id
  end
  def get_result_by_client(client_id)
    @lock.with_read_lock do
      @task_result_by_client.has_key? client_id or raise "Client ID #{client_id} not found on this worker."
    end
  end
  def add_result(client_id, job_id, result)
    result.is_a? TaskResult or raise ArgumentError
    @lock.with_write_lock do
      @task_result[job_id] << result
      @task_result_by_client[client_id] << job_id unless @task_result_by_client[client_id].include? job_id
    end
  end
  def clear_result(job_id)  # This is different from delete_result while this keeps entry
    @lock.with_write_lock do
      @task_result[job_id].clear
    end
  end
  def delete_result(key={})
    key.has_key? :client_id or raise ArgumentError, "Can't delete without client_id"
    @lock.with_write_lock do
      if key.has_key? :client_id and key.has_key? :job_id_list
        @task_result.delete_if{|k,v| (@task_result_by_client[:client_id] & key[:job_id_list]).include? k}
      elsif key.has_key? :client_id
        @task_result.delete_if{|k,v| key[:client_id].map{|c|task_result_by_client[c]}.flatten.include? k}
      else
        raise NotImplementedError
      end
    end
  end
end

class Worker < BaseServer
  attr_reader :name, :status, :id, :avg_running_time
  attr_writer :status_checker

  LEARNING_RATE = 0.2         # Rate of updating avg exec time

  module STATUS
    DOWN       = :DOWN
    UNKNOWN    = :UNKNOWN
    OCCUPIED   = :OCCUPIED    # Assigned to a job but not running a task
    AVAILABLE  = :AVAILABLE   # Idle, not assigned to a job
    BUSY       = :BUSY        # Running a task
  end

  def initialize(name, arg={})
    super arg[:logger]
    @id = SecureRandom::uuid()
    @name = name
    @lock = Mutex.new
    @status = STATUS::AVAILABLE
    @avg_running_time = nil
  end

  def register()
    @logger.info "Notifies status checker for coming"
    @status_checker.register_worker @name
  end

  def status=(s)
    raise ArgumentError if !STATUS::constants.include? s
    @logger.info "Worker status set to #{s}"
    @status = s
  end

  def log_running_time(job_id, time)
    @logger.info "Logs runtime to self"
    @avg_running_time = @avg_running_time == nil ? time : @avg_running_time * (1-LEARNING_RATE) + time * LEARNING_RATE
    @logger.info "Logs runtime to status_checker"
    @status_checker.log_running_time job_id, time
  end

  def release()
    # TODO: what if maliciously called?
    @status_checker.release_worker @name
  end

  def run_task(task, job_uuid=nil)
    if !task.is_a? Task
      @logger.error "The input is not a task"
      raise 'Not a proper task to run'
    end
    res = nil
    @lock.synchronize{  # Worker is dedicated
      @status_checker.worker_running @name
      @logger.info "Running task of job #{job_uuid}"
      res, elapsed= run_cmd(task.cmd, *task.args)
      @logger.info "Finished task of job #{job_uuid} in #{elapsed} seconds"
    }
    return res
  end

  def run_cmd(command, *args)
    @logger.debug "Running `#{command}#{args.join(' ')}`"
    start = Time.now
    # Should use wait_thr instead of $?; $? not working when using DRb
    stdin, stdout, stderr, wait_thr = Open3.popen3(command, *args)  #TODO: Possible with a chroot?
    result = {
      :stdout => stdout.readlines.join(''),
      :stderr => stderr.readlines.join(''),
      :exit_status => wait_thr.value
    }
    stdin.close
    stdout.close
    stderr.close
    elapsed = Time.now - start
    result[:elapsed] = elapsed
    @logger.debug "`#{command}#{args.join(' ')}` done in #{elapsed} seconds"
    return result, elapsed
  end

end

