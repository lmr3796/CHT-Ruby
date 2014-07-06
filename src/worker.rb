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
    @mutex = Mutex.new
    @status = STATUS::UNKNOWN
    @avg_running_time = nil
    @result_manager = TaskResultManager.new
    @dispatcher = arg[:dispatcher]
    @status_checker = arg[:status_checker]
    @assignment = Atomic.new(JobAssignment.new)
    return
  end

  def register()
    @logger.debug "Notifies status checker for coming"
    @status_checker.register_worker @name
    return
  rescue => e
    @logger.error "Error registering worker"
    @logger.error e.message
    @logger.error e.backtrace.join("\n")
  end

  def status=(s)
    raise ArgumentError if !STATUS::constants.include? s
    raise ArgumentError if s == STATUS::DOWN || s == STATUS::UNKNOWN # Insane to mark self as DOWN...
    @status = s
    @logger.info "Worker status set to #{s}"
    @status_checker.mark_worker_status(@name, @status)
    @logger.debug "Notified status checker for worker status set to #{s}"

    # Refactor this to a callback table if necessary; currently not.
    @dispatcher.on_worker_available(@name) if s == STATUS::AVAILABLE
    return s
  end

  def assignment
    return @assignment.value
  end

  def assignment=(a)
    a.is_a? JobAssignment or raise ArgumentError
    @assignment.update{|_| a}
    @logger.debug "Assigned with job:#{a.job_id}, client:#{a.client_id}"

    self.status = STATUS::OCCUPIED

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

  def release(client_id) # Should only be invoked by client
    if self.assignment.client_id != client_id
      @logger.error("Invalid caller client: #{client_id}")
      return false
    end
    self.status = STATUS::AVAILABLE
    return true
  end

  def validate_occupied_assignment
    return if @status != STATUS::OCCUPIED
    @mutex.synchronize do
      self.status = STATUS::AVAILABLE if @dispatcher.has_job?(self.assignment.job_id)
    end
  end

  def submit_task(task, client_id)
    task.is_a? Task or raise ArgumentError
    @mutex.synchronize do
      self.assignment.job_id == task.job_id or raise 'Job ID mismatch'
      self.assignment.client_id == client_id or raise 'Client ID mismatch'
      @assignment.update{|v| v.task = task; v}
      @logger.warn "#{task.job_id}[#{task.id}] submitted"
      Thread::main.run
    end
    # FIXME must make sure assignment not overriden before executed!
    return
  end

  def start
    loop do
      task = nil
      client_id = nil
      Thread.stop
      @mutex.synchronize do  # Worker is dedicated
        self.status = STATUS::BUSY
        task, client_id = [self.assignment.task, self.assignment.client_id]
        result = run_task(task)
        @result_manager.add_result(client_id, result)
      end
      @dispatcher.push_message(client_id, MessageService::Message.new(:task_result_available,
                                                                      :worker => @name,
                                                                      :job_id => task.job_id,
                                                                      :task_id => task.id))
      self.status = STATUS::AVAILABLE
    end
  end

  def run_task(task) task.is_a? Task or raise 'Invalid task to run'
    @logger.warn "#{task.job_id}[#{task.id}] running."
    result = TaskResult.new(task.id, task.job_id, run_cmd(task.cmd, *task.args))
    @logger.debug result.inspect
    log_running_time(task.job_id, result.run_time)
    @logger.debug "Finished task of job #{task.job_id} in #{result.run_time} seconds"
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
    return @result_manager.clear_result(clear_request, @logger)
  end

  def log_running_time(job_id, time)
    @avg_running_time = @avg_running_time == nil ? time : @avg_running_time * (1-LEARNING_RATE) + time * LEARNING_RATE
    @status_checker.log_running_time(job_id, time)
    return
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

  def clear_result(clear_request, logger)
    raise ArgumentError if !clear_request.is_a? Worker::ClearResultRequest
    @rwlock.with_write_lock do
      clear_request.execute(@task_result, @job_id_by_client, logger)
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
  def execute(task_result, job_id_by_client, logger)
    @to_delete.select{|job_id,_|job_id_by_client[@client_id].include? job_id}.each do |job_id, t_id_list|
      task_result[job_id] == [] and next if t_id_list == ALL
      task_result[job_id].reject!{|r|
        if t_id_list.include? r.task_id
          logger.warn "#{job_id}[#{r.task_id}] deleted."
          true
        else
          false
        end
      } # TODO Higher efficiency?
    end
  end
end
