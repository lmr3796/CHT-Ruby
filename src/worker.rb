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
class Worker::PreemptedError < RuntimeError; end

Worker::JobAssignment = Struct.new(:job_id, :client_id)

module Worker::STATUS
  DOWN       = :DOWN
  UNKNOWN    = :UNKNOWN
  OCCUPIED   = :OCCUPIED    # Assigned to a job but not running a task
  AVAILABLE  = :AVAILABLE   # Idle, not assigned to a job
  BUSY       = :BUSY        # Running a task
end

# TODO: refactor to state machine pattern
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
    @preemption_lock = Mutex.new
    @status = STATUS::UNKNOWN
    @avg_running_time = nil
    @result_manager = TaskResultManager.new
    @dispatcher = arg[:dispatcher]
    @status_checker = arg[:status_checker]
    @uri = arg[:uri]
    @assignment = Atomic.new(JobAssignment.new) # Real task assignment should not depend on this, this is only for validating submission
    @task_execution_thr = Thread::main
    return
  end

  def register()
    @logger.debug "Notifies status checker for coming"
    @status_checker.register_worker @name
    return
  rescue DRb::DRbConnError
    @logger.error "Error reaching the management system on registration."
    @status = STATUS::AVAILABLE   # Mark as available by itself if fail on registration
    @logger.warn "Mark status as AVAILABLE by self."
  end

  def fetch_assignment
    return @dispatcher.on_worker_available(@name)
  rescue DRb::DRbConnError
    @logger.error "Can't reach dispatcher to fetch assignment."
    return nil
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
    when STATUS::AVAILABLE
      fetch_assignment
    end
    return s
  end

  def assignment
    return @assignment.value.clone
  end

  # Should only be invoked while Dispatcher#on_worker_available, so there
  # shouldn't be any race condition here.
  def assignment=(a)
    @mutex.synchronize do
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
      @task_execution_thr.run
    end
    return
    end
  end

  def tell_client_ready(client_id, job_id)
    # Send message to client and tell ready
    raise ArgumentError if client_id == nil
    raise ArgumentError if job_id == nil
    worker_available_msg = MessageService::Message.new(:worker_available,
                                                       :worker=>@name,
                                                       :job_id=>job_id,
                                                       :uri => @uri)
    @logger.debug "Send message to tell client #{client_id} worker I'm ready to for job #{job_id}."
    @dispatcher.push_message(client_id, worker_available_msg)
    return
  end

  def awake                         # AVAILABLE ONLY
    @logger.warn "Not available, can't be awoken" and return if @status != STATUS::AVAILABLE
    @logger.debug "Awoken to fetch new assignment"
    next_job_assigned = fetch_assignment
    @logger.debug "Fetched #{next_job_assigned.inspect}" if next_job_assigned != nil
    return
  end

  def validate_occupied_assignment  # OCCUPIED ONLY
    @logger.warn "Not occupied, no need to validate" and return true if @status != STATUS::OCCUPIED
    @logger.debug "Validating assignment"
    @mutex.synchronize do
      valid = @dispatcher.get_assigned_job(@name) == self.assignment.job_id
      @logger.debug("Assignment of job #{self.assignment.job_id} is #{valid ? 'valid' : 'invalid'}.")
      if !valid
        @logger.debug("Release on self validation")
        @task_execution_thr.raise(InvalidAssignmentError)
      end
      return valid
    end
  rescue DRb::DRbConnError
    @logger.error "Can't reach dispatcher to validate assignment."
  end

  def release(client_id, job_id)            # Should only be invoked by client on OCCUPIED
    if self.assignment.client_id != client_id || self.assignment.job_id != job_id
      @logger.error("Invalid caller client: #{client_id}")
      return false
    end
    @logger.warn "Releasing by #{client_id}:#{job_id}"
    @task_execution_thr.raise(InvalidAssignmentError)
    @logger.warn "Released by #{client_id}:#{job_id}"
    return
  end

  def submit_task(task, client_id)
    raise ArgumentError if !task.is_a? Task
    raise WorkerStateCorruptError if self.status != STATUS::OCCUPIED
    raise WorkerStateCorruptError if !@task_execution_thr.stop?

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
      raise WorkerStateCorruptError if !@task_execution_thr.stop?
      @task_execution_thr[:task] = task
      @task_execution_thr[:client_id] = client_id
      @task_execution_thr.raise WorkerStateCorruptError and raise WorkerStateCorruptError if !@task_execution_thr.stop?
      raise WorkerStateCorruptError if self.status != STATUS::OCCUPIED
      raise WorkerStateCorruptError if !@task_execution_thr.stop?
      @logger.debug "#{task.job_id}[#{task.id}] submitted"
      self.status = STATUS::BUSY
      @task_ready.signal
      return true
    end
  rescue WorkerStateCorruptError => e
    @logger.error e.message
    @logger.error "Status = #{self.status}"
    @logger.error "Execution thread task status = #{@task_execution_thr.status}"
    @logger.error "Execution thread task = #{@task_execution_thr[:task].inspect}"
    @logger.error e.backtrace.join("\n")
    return false
  end

  def validate_state_after_client_submission
    raise WorkerStateCorruptError, "Not waken up by client submitting task" if @status != STATUS::BUSY
    raise WorkerStateCorruptError, "No task submitted from client but runs." if Thread.current[:task] == nil
    raise WorkerStateCorruptError, "No task submitted from client but runs." if Thread.current[:client_id] == nil
  end
  private :validate_state_after_client_submission

  # FIXME: Preemption lock is a dirty hack...
  def start
    loop do
      @preemption_lock.lock unless @preemption_lock.owned?
      Thread::stop if @status == STATUS::AVAILABLE
      @mutex.synchronize do  # Worker is dedicated
        begin
          # Must be OCCUPIED here and client waits for client to submit a task
          raise WorkerStateCorruptError, "Status should be OCCUPIED" if @status != STATUS::OCCUPIED

          a = self.assignment
          @task_ready = ConditionVariable.new
          tell_client_ready(a.client_id, a.job_id)
          #Timeout::timeout(DEFAULT_TIMEOUT)  # TODO: Maybe enable this in production....
          @task_ready.wait(@mutex)            # FIXME: might get waken by #assignment=
          validate_state_after_client_submission

          # Task submitted. Run!!!
          task, client_id = [Thread.current[:task], Thread.current[:client_id]]
          begin
            @preemption_lock.unlock
            result = run_task(task)
            @preemption_lock.lock unless @preemption_lock.owned?
          rescue ThreadError => e
            @logger.error e.message
            @logger.error e.backtrace.join("\n")
            system('killall ruby')
          end
          log_running_time(result.job_id, result.run_time)
          @logger.debug "Finished #{result.job_id}[#{result.task_id}] in #{result.run_time} seconds"
          @result_manager.add_result(client_id, result)

          # Task done
          begin
            @logger.debug "Notify dispacher #{result.job_id}[#{result.task_id}] done."
            @dispatcher.task_done(result.job_id)
            @logger.debug "Send message to tell client #{client_id} #{result.job_id}[#{result.task_id}] done."
            @dispatcher.push_message(client_id, MessageService::Message.new(:task_result_available,
                                                                            :worker => @name,
                                                                            :job_id => task.job_id,
                                                                            :task_id => task.id))
          rescue DRb::DRbConnError
            @logger.error "Error when notifing dispacher #{result.job_id}[#{result.task_id}] done."
          end
          raise WorkerStateCorruptError, "Status should be BUSY" if @status != STATUS::BUSY
        rescue PreemptedError
          @logger.warn "#{task.job_id}[#{task.id}] is preempted."
          @dispatcher.push_message(client_id, MessageService::Message.new(:task_preempted,
                                                                          :worker => @name,
                                                                          :job_id => task.job_id,
                                                                          :task_id => task.id))
        rescue Timeout::Error
          @logger.warn "Waited too long for assignment #{self.assignment.inspect}"
        rescue InvalidAssignmentError
          @logger.warn "Assignment of job #{self.assignment.job_id} invalid, release."
          # FIXME: What if interrupted here...
        rescue WorkerStateCorruptError => e
          @logger.fatal e.message
          @logger.fatal e.backtrace.join("\n")
          system('killall ruby') # FIXME: Remove this after fixing state corrupt bug; debug use!!!
        rescue => e
          @logger.error e.message
          @logger.error e.backtrace.join("\n")
        ensure
          # Can't set them to nil before preemption done
          @preemption_lock.lock unless @preemption_lock.owned?
          Thread.current[:task] = Thread.current[:client_id] = nil
        end
      end
      self.status = STATUS::AVAILABLE # This triggers pulling next assignment from dispatcher
    end
  end

  def run_task(task)
    @logger.fatal task.inspect and raise 'Invalid task to run' if !task.is_a? Task
    @logger.info "#{task.job_id}[#{task.id}] running."
    @logger.info "Running `#{task.cmd} #{task.args.join(' ')}`"
    result = task.run
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

  def current_execution_of(client_id)
    return Thread.exclusive do
      client_id != @task_execution_thr[:client_id] ? nil : [@task_execution_thr[:task].job_id, @task_execution_thr[:task].id]
    end
  end

  def exist_result?(job_id, task_id, client_id)
    @result_manager.exist_result?(job_id, task_id, client_id)
  end

  def preempt_if_unmatch(job_id)
    return unless @preemption_lock.try_lock # If we can't lock it, it means it's not running a task
    @task_execution_thr.raise(PreemptedError) if @task_execution_thr[:task].job_id != job_id
    @preemption_lock.unlock
    return
  rescue ThreadError => e
    @logger.error e.message
    @logger.error e.backtrace.join("\n")
    system('killall ruby')
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

  def exist_result?(job_id, task_id, client_id)
    @rwlock.with_read_lock do
      return false if @job_id_by_client[client_id] == nil
      return false if !@job_id_by_client[client_id].include? job_id
      return @task_result[job_id].any?{|r| r.task_id == task_id}
    end
  end
end

class Worker::ClearResultRequest
  attr_reader :client_id, :task_id_to_delete, :job_id
  ALL = :ALL
  def initialize(job_id, delete_list, client_id)
    @job_id = job_id
    @client_id = client_id
    self.task_id_to_delete = delete_list
  end

  def task_id_to_delete=(delete_list)
    raise ArgumentError if !delete_list.is_a? Enumerable || delete_list == ALL
    raise ArgumentError, 'No jobs to delete' if delete_list.empty?
    @task_id_to_delete = delete_list
  end

  # Command Pattern
  def execute(task_result, job_id_by_client, logger)
    if !job_id_by_client[@client_id].include? @job_id
      logger.warn "Invalid clear request that #{@client_id} to delete #{@job_id}"
      return
    end
    task_result[job_id] = [] and return if @task_id_to_delete == ALL
    task_result[job_id].reject! do |r|
      if @task_id_to_delete.include? r.task_id
        logger.debug "#{job_id}[#{r.task_id}] deleted."
        true
      else
        false
      end
    end
    return
  end
end

class SimulatedHeterogeneousWorker < Worker
  attr_accessor :argument

  def initialize(name, args={})
    super
    # TODO: receive the arguments of the distribution of actual sleeping time
    self.argument = args[:simulation_argument]
  end

  def argument=(a)
    raise ArgumentError if !a.is_a? Numeric
    @argument = a
  end

  # TODO: Change this model
  def additional_sleep_time(task)
    raise ArgumentError if !task.is_a? Task
    return 0.0 if !task.is_a? SleepTask
    return Random.rand(@argument) * task.sleep_time
  end

  def run_task(task)
    @logger.fatal task.inspect and raise 'Invalid task to run' if !task.is_a? Task
    @logger.debug "#{task.job_id}[#{task.id}] running."
    @logger.debug "Running `#{task.cmd} #{task.args.join(' ')}` in simulated heterogeneous environment"

    # Additional sleep to simulate
    hetero_time = additional_sleep_time(task)
    @logger.info "Sleep for an additional #{hetero_time} seconds"
    sleep hetero_time

    # Execution
    result = super(task)

    # Synthesizing the result
    result.run_time += hetero_time

    return result
  end
end
