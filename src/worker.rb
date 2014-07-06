require 'open3'
require 'logger/colors'
require 'time'
require 'thread'
require 'securerandom'

require_relative 'common/rwlock'

require_relative 'base_server'
require_relative 'job'
require_relative 'message_service'

class Worker < BaseServer; end

Worker::JobAssignment = Struct.new(:job_id, :client_id, :task)

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
    @dispatcher = arg[:dispatcher]
    @status_checker = arg[:status_checker]
    @assignment = Atomic.new(JobAssignment.new)
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


  # Should only be invoked by client
  def release(client_id)
    if @assignment.value.client_id != client_id
      @logger.error("Invalid caller client: #{client_id}")
      return false 
    end
    @status_checker.release_worker(@name, false)  # For preventing it from looping
    return true
  end

  def assign_job(assignment)
    assignment.is_a? JobAssignment or raise ArgumentError
    @assignment.update{|_| assignment} 
    self.status = STATUS::OCCUPIED
    @logger.debug "Assigned with job:#{assignment.job_id}, client:#{assignment.client_id}"

    # Send message to client and tell ready
    client = @assignment.value.client_id
    job_id = @assignment.value.job_id
    worker_available_msg = MessageService::Message.new(:worker_available,
                                                       :worker=>@name,
                                                       :job_id=>job_id)
    @logger.debug "Send message to tell client #{client} worker I'm ready to for job #{job_id}."
    @dispatcher.push_message(client, worker_available_msg)
    return
  end

  def log_running_time(job_id, time)
    @logger.info "Logs runtime to self"
    @avg_running_time = @avg_running_time == nil ? time : @avg_running_time * (1-LEARNING_RATE) + time * LEARNING_RATE
    @logger.info "Logs runtime to status_checker"
    @status_checker.log_running_time job_id, time
    return
  end

  def submit_task(task, client_id)
    task.is_a? Task or raise ArgumentError
    @lock.synchronize do
      @assignment.value.job_id == task.job_id or raise 'Job ID mismatch'
      @assignment.value.client_id == client_id or raise 'Client ID mismatch'
      @assignment.update{|v| v.task = task; v}
      Thread::main.run
    end
    return
  end

  def start
    loop do
      Thread.stop if @task_to_run == nil
      $stderr.puts "Awake"
      task, client_id = [@assignment.value.task, @assignment.value.client_id]
      @lock.synchronize do  # Worker is dedicated
        result = run_task(task)
        @status = STATUS::AVAILABLE
        @result_manager.add_result(client_id, result)
      end
      @dispatcher.push_message(client_id, MessageService::Message.new(:task_result_available,
                                                                      :worker => @name,
                                                                      :job_id => task.job_id,
                                                                      :task_id => task.id))
      # TODO: Convert this call into message?
      @status_checker.on_task_done(@name, task.id, task.job_id, client_id)
    end
  end

  def run_task(task) task.is_a? Task or raise 'Invalid task to run'
    @status_checker.worker_running(@name)
    @logger.info "Running task of job #{task.job_id}"
    result = TaskResult.new(task.id, task.job_id, run_cmd(task.cmd, *task.args))
    @logger.debug result.inspect
    log_running_time(task.job_id, result.run_time)
    @logger.info "Finished task of job #{task.job_id} in #{result.run_time} seconds"
    return result
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

  # TODO: lower the cost of get_result
  def get_results(client_id, job_id)
    return @result_manager.get_result_by_client(client_id)[job_id]
  end

  def clear_result(clear_request) # Delegator
    clear_request.is_a? Worker::ClearResultRequest or raise ArgumentError
    return @result_manager.clear_result(clear_request)
  end
end

class Worker::TaskResultManager
  def initialize()
    @rwlock = ReadWriteLock.new
    # Following field won't be written concurrently
    @task_result = Hash.new
    @job_id_by_client = Hash.new # Contains job_id
  end

  def get_result_by_client(client_id)
    @rwlock.with_read_lock do
      @job_id_by_client.has_key? client_id or raise "Client ID #{client_id} not found on this worker."
      return @task_result.select{|k,v|@job_id_by_client[client_id].include? k}
    end
  end

  def add_result(client_id, result)
    result.is_a? TaskResult or raise ArgumentError
    @rwlock.with_write_lock do
      @task_result[result.job_id] ||= []
      @task_result[result.job_id] << result
      @job_id_by_client[client_id] ||= []
      @job_id_by_client[client_id].include? result.job_id or
        @job_id_by_client[client_id] << result.job_id
    end
    return
  end

  def clear_result(clear_request)
    raise ArgumentError if !clear_request.is_a? Worker::ClearResultRequest
    @rwlock.with_write_lock do
      clear_request.execute(@task_result, @job_id_by_client)
    end
    return
  end
end

class Worker::ClearResultRequest
  class NoJobToDeleteError < ArgumentError; end
  attr_reader :client_id, :to_delete
  ALL = :ALL
  def initialize(client_id, delete_table)
    @client_id = client_id
    self.to_delete = delete_table
  end

  def to_delete=(delete_table)
    raise ArgumentError if !delete_table.is_a? Hash
    raise NoJobToDeleteError, 'No jobs to delete' if delete_table.values.empty?

    delete_table.values.
      reject{|e| e == ALL || e.is_a?(Array)}.size == 0 or raise ArgumentError
    @to_delete = delete_table
  end

  # Command Pattern
  def execute(task_result, job_id_by_client)
    @to_delete.select do |job_id,_|
      job_id_by_client[@client_id].include? job_id
    end.each do |job_id, task_id_list|
      task_result[job_id] == [] and next if task_id_list == ALL
      task_result[job_id].reject!{|r| task_id_list.include? r} # TODO Higher efficiency?
    end
  end
end
