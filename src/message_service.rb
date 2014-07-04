require_relative 'common/rwlock_hash'

#FIXME: message service sometime fails
module MessageService
  class InvalidMessageError < StandardError; end
  class NoMatchingHandlerError < StandardError; end
  # TODO: maybe make it a struct???
  class Message
    attr_accessor :type, :content, :message
    def initialize(type, content=nil, message='')
      self.type = type
      self.content = content
      self.message = message
      return
    end

    def type=(t)
      t.is_a? Symbol or raise ArgumentError
      return @type = t
    end

    def content=(c)
      c == nil || c.is_a?(Hash) or raise ArgumentError
      return @content = c
    end

    def message=(m)
      m.is_a? String or raise ArgumentError
      return @message = m
    end
  end

  module Server
    def get_clients()
      raise NotImplementedError
    end

    def register(client_id)
      raise NotImplementedError
    end

    def unregister(client_id)
      raise NotImplementedError
    end

    def push_message(client_id, message)
      raise NotImplementedError
    end

    def broadcast_message(message)
      raise NotImplementedError
    end

    def get_message(client_id, timeout_limit=5)
      raise NotImplementedError
    end
  end

  class BasicServer
    include Server
    def initialize
      @client_message_queue = ReadWriteLockHash.new
      return
    end

    def get_clients()
      return @client_message_queue.keys
    end

    def register(client_id)
      @client_message_queue[client_id] = Queue.new
      return
    end

    def unregister(client_id)
      @client_message_queue[client.uuid].clear
      @client_message_queue.delete client.uuid
      return
    end

    def push_message(client_id, message)
      @client_message_queue[client_id] << message
      return
    end

    def broadcast_message(message)
      @client_message_queue.values.each{|q| q << message}
      return
    end

    def get_message(client_id, timeout_limit=5)
      msg = []
      Timeout::timeout(timeout_limit) do
        loop do # collect all as a batch
          msg << @client_message_queue[client_id].pop
          break if @client_message_queue[client_id].empty?
        end
      end
    rescue Timeout::Error  #This rescue is very necessary since DRb seems to catch it outside :P
    ensure
      return msg
    end
  end

  class Client
    attr_accessor :logger

    def initialize(uuid, msg_server, handler)
      uuid.is_a? String or raise ArgumentError
      handler.is_a? MessageHandler or raise ArgumentError
      @msg_queue = Queue.new
      @uuid = uuid
      @msg_server = msg_server
      @handler = handler
      @logger = Logger.new(STDERR)

      # Producer && consumer
      @notification_thr = Thread.new do
        Thread.stop # Don't run immediately, wait for client to start
        begin
          poll_message
        rescue => e
          @logger.fatal "Error on polling message"
          @logger.fatal e.message
          @logger.fatal e.backtrace.join("\n")
          retry
        end
      end
      @process_thr = Thread.new do
        Thread.stop # Don't run immediately, wait for client to start
        begin
          process_message_queue
        rescue => e
          @logger.fatal "Error on processing message queue"
          @logger.fatal e.message
          @logger.fatal e.backtrace.join("\n")
          retry
        end
      end
      # Wait until they're sleeping, otherwise start might let them directly execute stop...
      until @notification_thr.stop? && @process_thr.stop? do sleep 1 end
      return
    end

    def start
      @notification_thr.run
      @process_thr.run
      # TODO test if msg service connected
      return
    end

    def stop
      @notification_thr.kill
      @process_thr.kill
      return
    end

    def poll_message
      loop do
        # Timeout must be implemented on server side since drb won't release wait on error...
        msg_list = @msg_server.get_message(@uuid)
        begin
          msg_list.is_a? Array or raise 'Obtained stuff should be a list of Message from server'
          @logger.debug "Retrieved #{msg_list.size} messages from server" if msg_list.size > 0
          next if msg_list.empty?
          msg_list.each do |m|
            @logger.debug "Push message #{m.inspect} to message queue"
            @msg_queue << m
          end
        rescue InvalidMessageError
          @logger.error "Invalid stuff obtained: #{msg_list}"
          next
        end
      end
    end

    def process_message(m)
      m.is_a? Message or raise InvalidMessageError
      @logger.debug "Received #{m}"
      handler_name = "on_#{m.type.to_s}"
      @handler.respond_to?(handler_name) or raise NoMatchingHandlerError
      @logger.debug "Send #{m} to #{handler_name}"
      @handler.send(handler_name, m)  # The ruby way to invoke method by its name string
      return
    end

    def process_message_queue
      loop do
        m = @msg_queue.pop
        begin
          process_message(m)
        rescue InvalidMessageError => e
          @logger.error "Message #{m} invalid"
        rescue NoMatchingHandlerError => e
          @handler.on_no_handler_found_error(m, e)
        rescue => e
          @logger.error "Error processing message #{m.inspect}"
          @logger.error e.message
          @logger.error e.backtrace.join("\n")
        end
      end
    end

    private :process_message
  end

  module Client::MessageHandler # Message Handler Interface to expose
    def on_chat(m)  # For testing :P
      @logger.debug "on_chat: Received \"#{m.message}\""
      return
    end

    def on_no_handler_found_error(m, e)
      @logger.warn "No handler for message #{m}"
      return
    end
  end
end
