require 'open3'
require 'logger/colors'
require 'time'
require 'timeout'
require 'thread'
require 'securerandom'

require_relative 'common/rwlock'

require_relative 'base_server'
require_relative 'job'
require_relative 'message_service'

class Worker < BaseServer; end
class Worker::WorkerStateCorruptError < RuntimeError; end
class Worker::InvalidAssignmentError < RuntimeError; end

Worker::JobAssignment = Struct.new(:job_id, :client_id)

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
  DEFAULT_TIMEOUT = 5

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
    @assignment = Atomic.new(JobAssignment.new) # Real task assignment should not depend on this, this is only for validating submission
    @task_ready = ConditionVariable.new
    @occupied = ConditionVariable.new
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
    case s
    when STATUS::OCCUPIED
      @occupied.signal
    when STATUS::AVAILABLE
      @dispatcher.on_worker_available(@name)
    end
    return s
  end

  def assignment
    return @assignment.value
  end

  # Should only be invoked while on_worker_available, so there
  # shouldn't be any race condition here.
  def assignment=(a)
    a.is_a? JobAssignment or raise ArgumentError
    @assignment.update do |_|
      @logger.debug "Assigned with job:#{a.job_id}, client:#{a.client_id}"
      a
    end

    #TODO: Refactor: make client messages as a queue and future value.
    # So we can process it in main thread. The code will become cleaner
    if @status == STATUS::AVAILABLE # This if checking is very critical. It may corrupt condition variable waiting to be corrupted
      self.status = STATUS::OCCUPIED
      @logger.debug "Notifies main thread to keep executing"
      Thread::main.run
    end
    return
  end

  def tell_client_ready(client_id, job_id)
    # Send message to client and tell ready
    raise ArgumentError if client_id == nil
    raise ArgumentError if job_id == nil
    worker_available_msg = MessageService::Message.new(:worker_available,
                                                       :worker=>@name,
                                                       :job_id=>job_id)
    @logger.debug "Send message to tell client #{client_id} worker I'm ready to for job #{job_id}."
    @dispatcher.push_message(client_id, worker_available_msg)
    return
  end

  def release(client_id)            # Should only be invoked by client
    if self.assignment.client_id != client_id
      @logger.error("Invalid caller client: #{client_id}")
      return false
    end
    self.status = STATUS::AVAILABLE
    return true
  end

  def awake                         # AVAILABLE ONLY
    @logger.warn "Not available, can't be awoken" and return if @status != STATUS::AVAILABLE
    @logger.debug "Awoken to fetch new assignment"
    next_job_assigned = @dispatcher.on_worker_available(@name)
    @logger.debug "Fetched #{next_job_assigned.inspect}"
    return
  end

  def validate_occupied_assignment  # OCCUPIED ONLY
    @logger.warn "Not occupied, no need to validate" and return true if @status != STATUS::OCCUPIED
    @logger.debug "Validating assignment"
    valid = @mutex.synchronize{@dispatcher.has_job?(self.assignment.job_id)}
    valid ?
      @logger.debug("Assignment of job #{self.assignment.job_id} valid.") :
      Thread::main.raise(InvalidAssignmentError)
    return valid
  end

  def submit_task(task, client_id)
    raise ArgumentError if !task.is_a? Task
    raise WorkerStateCorruptError if self.status != STATUS::OCCUPIED
    raise WorkerStateCorruptError if !Thread::main.stop?

    @mutex.synchronize do
      a = self.assignment
      if a.job_id != task.job_id
        @logger.warn "Job ID mismatch: assigned #{a.job_id} but submitted #{task.job_id}"
        return false
      end
      if a.client_id != client_id
        @logger.warn "Client ID mismatch: assigned #{a.client_id} but #{client_id} is submitting"
        return false
      end
      raise WorkerStateCorruptError if self.status != STATUS::OCCUPIED
      raise WorkerStateCorruptError if !Thread::main.stop?
      Thread::main[:task] = task
      Thread::main[:client_id] = client_id
      Thread::main.raise WorkerStateCorruptError and raise WorkerStateCorruptError if !Thread::main.stop?
      raise WorkerStateCorruptError if self.status != STATUS::OCCUPIED
      raise WorkerStateCorruptError if !Thread::main.stop?
      @logger.debug "#{task.job_id}[#{task.id}] submitted"
      self.status = STATUS::BUSY
      @task_ready.signal
      return true
    end
  end

  def validate_state_after_client_submission
    raise WorkerStateCorruptError, "Not waken up by client submitting task" if @status != STATUS::BUSY
    raise WorkerStateCorruptError, "No task submitted from client but runs." if Thread.current[:task] == nil
    raise WorkerStateCorruptError, "No task submitted from client but runs." if Thread.current[:client_id] == nil
  end
  private :validate_state_after_client_submission

  def start
    loop do
      Thread::stop if @status == STATUS::AVAILABLE
      @mutex.synchronize do  # Worker is dedicated
        begin
          # Must be OCCUPIED here and client waits for client to submit a task
          raise WorkerStateCorruptError, "Status should be OCCUPIED" if @status != STATUS::OCCUPIED

          a = self.assignment
          tell_client_ready(a.client_id, a.job_id)
          #Timeout::timeout(DEFAULT_TIMEOUT)  # TODO: Maybe enable this in production....
          @task_ready.wait(@mutex)            # FIXME: might get waken by #assignment=
          validate_state_after_client_submission

          # Task submitted. Run!!!
          task, client_id = [Thread.current[:task], Thread.current[:client_id]]
          Thread.current[:task] = Thread.current[:client_id] = nil
          result = run_task(task)
          @result_manager.add_result(client_id, result)
          @dispatcher.push_message(client_id, MessageService::Message.new(:task_result_available,
                                                                          :worker => @name,
                                                                          :job_id => task.job_id,
                                                                          :task_id => task.id))
          raise WorkerStateCorruptError, "Status should be BUSY" if @status != STATUS::BUSY

        rescue InvalidAssignmentError
          @logger.warn "Assignment of job #{self.assignment.job_id} invalid, release."
        rescue WorkerStateCorruptError => e
          @logger.fatal e.message
          @logger.fatal e.backtrace.join("\n")
          system('killall ruby') # FIXME: Remove this after fixing state corrupt bug; debug use!!!
        rescue Timeout::Error
          @logger.warn "Waited too long for assignment #{self.assignment.inspect}"
          next
        rescue => e
          @logger.error e.message
          @logger.error e.backtrace.join("\n")
          system('killall ruby') # FIXME: Remove this after fixing state corrupt bug; debug use!!!
        ensure
          self.status = STATUS::AVAILABLE # This triggers pulling next assignment from dispatcher
        end
      end
    end
  end

  def run_task(task)
    @logger.fatal task.inspect and raise 'Invalid task to run' if !task.is_a? Task
    @logger.debug "#{task.job_id}[#{task.id}] running."
    @logger.info "Running `#{task.cmd} #{task.args.join(' ')}`"
    result = task.run
    log_running_time(result.job_id, result.run_time)
    @logger.debug "Finished #{result.job_id}[#{result.task_id}] in #{result.run_time} seconds"
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
          logger.debug "#{job_id}[#{r.task_id}] deleted."
          true
        else
          false
        end
      } # TODO Higher efficiency?
    end
  end
end
