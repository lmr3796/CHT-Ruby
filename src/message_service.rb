require_relative 'common/rwlock_hash'

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
    end

    def type=(t)
      t.is_a? Symbol or raise ArgumentError
      @type = t
    end

    def content=(c)
      c == nil || c.is_a?(Hash) or raise ArgumentError
      @content = c
    end

    def message=(m)
      m.is_a? String or raise ArgumentError
      @message = m
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
    end

    def get_clients()
      @client_message_queue.keys
    end

    def register(client_id)
      @client_message_queue[client_id] = Queue.new
    end

    def unregister(client_id)
      @client_message_queue[client.uuid].clear
      @client_message_queue.delete client.uuid
    end

    def push_message(client_id, message)
      @client_message_queue[client_id] << message
    end

    def broadcast_message(message)
      @client_message_queue.values.each{|q| q << message}
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

    module MessageHandler # Message Handler Interface to expose
      def on_chat(m)  # For testing :P
        @logger.debug "on_chat: Received \"#{m.message}\""
      end

      def on_no_handler_found_error(m, e)
        @logger.warn "No handler for message #{m}"
      end
    end

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
        poll_message
      end
      @process_thr = Thread.new do
        Thread.stop # Don't run immediately, wait for client to start
        process_message_queue
      end
    end

    def << (m)
      @msg_queue << m
    end

    def start
      @notification_thr.run
      @process_thr.run
      # TODO test if msg service connected
    end

    def stop
      @notification_thr.kill
      @process_thr.kill
    end

    def poll_message
      loop do
        # Timeout must be implemented on server side since drb won't release wait on error...
        msg = @msg_server.get_message @uuid
        next if msg.empty?
        msg.each {|m| @msg_queue << m}
      end
    end

    def process_message(m)
      m.is_a? Message or raise InvalidMessageError
      handler_name = "on_#{m.type.to_s}"
      @handler.respond_to?(handler_name) or raise NoMatchingHandlerError
      @handler.send(handler_name, m)  # The ruby way to invoke method by its name string
    end

    def process_message_queue
      loop do
        m = @msg_queue.pop
        m.is_a? Message or raise InvalidMessageError
        begin
          process_message(m)
        rescue InvalidMessageError => e
          @logger.error "Message #{m} invalid"
        rescue NoMatchingHandlerError => e
          @handler.on_no_handler_found_error(m, e)
        rescue => e
          @logger.error "Error processing message #{m}"
          @logger.error e.message
          @logger.error e.backtrace.join('\n')
        end
      end
    end

    private :process_message
  end
end
