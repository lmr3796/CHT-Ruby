
class MessageService
  module MessageHandler
    def logger
      raise NotImplementedError
    end
  end
  def initialize(uuid, dispatcher, handler)
    uuid.is_a? String or raise ArgumentError
    handler.is_a? MessageHandler or raise ArgumentError
    @msg_queue = Queue.new
    @uuid = uuid
    @dispatcher = dispatcher
    @handler = handler
    @logger = @handler.logger

    # Producer && consumer
    @notification_thr = Thread.new do
      Thread.stop # Don't run immediately, wait for client to start
      poll_message
    end
    @process_thr = Thread.new do
      Thread.stop # Don't run immediately, wait for client to start
      process_message_queue
    end
    @logger.info "Initialized message service; uuid=#{@uuid}"
  end

  def << (m)
    @msg_queue << m
  end

  def start
    @logger.info "Running message service; uuid=#{@uuid}"
    @notification_thr.run
    @process_thr.run
  end

  def stop
    @notification_thr.kill
    @process_thr.kill
  end

  def poll_message
    loop do
      # Timeout must be implemented on server side since drb won't release wait on error...
      msg = @dispatcher.get_message @uuid
      next if msg.empty?
      msg.each {|m| @msg_queue << m}
    end
  end

  def process_message_queue
    loop do
      m = @msg_queue.pop
      begin
        handler_name = "on_#{m[:type].to_s}"
        @handler.respond_to?(handler_name) ?
          @handler.send(handler_name, m) :  # The ruby way to invoke method by its name string
          @logger.warn("No handler #{handler_name} for #{m[:type]} found, msg=#{m.inspect}")
      rescue => e
        @logger.warn("Error on parsing message, msg=#{m.inspect}")
        @logger.warn e.message
        @logger.warn e.backtrace.join("\n")
      end
    end
  end
end
