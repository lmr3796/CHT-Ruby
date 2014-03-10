require 'drb/drb'
require './config.rb'
require 'daemons'


class Dispatcher
end

Daemons.run_proc('dispatcher.rb') do
  SERVER_URI = CHT::Config::druby_uri(CHT::Config::DISPATCHER)
  FRONT_OBJECT = Dispatcher.new
  DRb.start_service(SERVER_URI, FRONT_OBJECT)
  DRb.thread.join
end
