require 'logger/colors'
require_relative 'server_monitor'

class BaseServer
  attr_writer :logger
  include ServerStatusChecking
  def initialize(logger=nil)
    @logger = logger || Logger.new(STDERR)
  end
  def logger=(f)
    @logger = f.is_a?(Logger) ? f : Logger.new(f)
  end
end
