require 'open3'
require 'logger/colors'
require 'time'
require 'thread'
require 'securerandom'

require_relative 'base_server'
require_relative 'job.rb'
require_relative 'common/rwlock'

class Worker < BaseServer; end

module Worker::STATUS
  DOWN       = :DOWN
  UNKNOWN    = :UNKNOWN
  OCCUPIED   = :OCCUPIED    # Assigned to a job but not running a task
  AVAILABLE  = :AVAILABLE   # Idle, not assigned to a job
  BUSY       = :BUSY        # Running a task
end

class Worker < BaseServer
  attr_reader :name, :status, :id, :avg_running_time
  attr_writer :status_checker

  LEARNING_RATE = 0.2         # Rate of updating avg exec time

  def initialize(name, arg={})
    super arg[:logger]
    @id = SecureRandom::uuid()
    @name = name
    @lock = Mutex.new
    @status = STATUS::AVAILABLE
    @avg_running_time = nil
    @result_manager = TaskResultManager.new
    @dispatcher = DRbObject.new_with_uri(arg[:dispatcher_uri])
    @status_checker = DRbObject.new_with_uri(arg[:status_checker_uri])
    @job_assignment = Atomic.new([nil,nil])
    return
  end

  def register()
    @logger.info "Notifies status checker for coming"
    @status_checker.register_worker @name
    return
  end

  def status=(s)
    raise ArgumentError if !STATUS::constants.include? s
    @logger.info "Worker status set to #{s}"
    @status = s
    return s
  end

  def log_running_time(job_id, time)
    @logger.info "Logs runtime to self"
    @avg_running_time = @avg_running_time == nil ? time : @avg_running_time * (1-LEARNING_RATE) + time * LEARNING_RATE
    @logger.info "Logs runtime to status_checker"
    @status_checker.log_running_time job_id, time
    return
  end

  def release()
    # TODO: what if maliciously called?
    @status_checker.release_worker @name
    return
  end

  def assign_job(assignment)
    def valid_assignment?(assignment)
      return assignment.is_a?(Array) && assignment.size == 2
    end
    valid_assignment?(assignment) or raise ArgumentError
    @job_assignment = assignment
    self.status = STATUS::OCCUPIED
    @logger.debug "Assigned with job:#{assignment[0]}, client:#{assignment[1]}"
    return
  end

  def submit_task(task, job_uuid, client_id)
    task.is_a? Task or raise ArgumentError
    # TODO verify job_uuid && client_id
    @lock.synchronize do  # Worker is dedicated
      @task_to_run = {:task => task, :job_uuid=>job_uuid, :client_id=>client_id}
    end
    Thread::main.run
    return
  end

  def start
    loop do
      Thread.stop if @task_to_run == nil
      $stderr.puts "Awake"
      @lock.synchronize do  # Worker is dedicated
        run_task(@task_to_run[:task], @task_to_run[:job_uuid], @task_to_run[:client_id])
        @task_to_run = nil
        # TODO notifies dispatcher for task completion
      end
    end
    return
  end

  def run_task(task, job_id, client_id)
    task.is_a? Task or raise ArgumentError
    @status_checker.worker_running @name
    @logger.info "Running task of job #{job_id}"
    result = TaskResult.new(task.id, job_id, run_cmd(task.cmd, *task.args))
    @logger.debug result.inspect
    log_running_time(job_id, result.run_time)
    @logger.info "Finished task of job #{job_id} in #{result.run_time} seconds"
    @result_manager.add_result(client_id, TaskResult.new(task.id, job_id, result))
    @dispatcher.on_task_done(@name, task_id, job_id, client_id)
    @status_checker.on_task_done(@name, task_id, job_id, client_id)
    return
  end

  def run_cmd(command, *args)
    @logger.debug "Running `#{command} #{args.join(' ')}`"
    start = Time.now
    # Should use wait_thr instead of $?; $? not working when using DRb
    stdin, stdout, stderr, wait_thr = Open3.popen3(command, *args)  #TODO: Possible with a chroot?
    result = {
      :stdout => stdout.readlines.join(''),
      :stderr => stderr.readlines.join(''),
      :status => wait_thr.value,
      :run_time => Time.now - start
    }
    @logger.debug "`#{command}#{args.join(' ')}` done in #{result[:run_time]} seconds"
    stdin.close
    stdout.close
    stderr.close
    return result
  end

  def get_results(client_id)
    return @result_manager.get_result_by_client(client_id)
  end

end

class Worker::TaskResultManager
  def initialize()
    @rwlock = ReadWriteLock.new
    # Following field won't be written concurrently
    @task_result = Hash.new {|h,k| h[k] = []}
    @task_result_by_client = Hash.new # Contains job_id
  end

  def get_result_by_client(client_id)
    @rwlock.with_read_lock do
      @task_result_by_client.has_key? client_id or raise "Client ID #{client_id} not found on this worker."
      return @task_result.select{|k,v|@task_result_by_client[client_id].include? k}
    end
  end

  def add_result(client_id, result)
    result.is_a? TaskResult or raise ArgumentError
    @rwlock.with_write_lock do
      @task_result[result.job_id] << result
      @task_result_by_client[client_id] << job_id unless @task_result_by_client[client_id].include? job_id
    end
  end

  def delete_result(key={})
    key.has_key? :client_id or raise ArgumentError, "Can't delete without client_id"
    @rwlock.with_write_lock do
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
