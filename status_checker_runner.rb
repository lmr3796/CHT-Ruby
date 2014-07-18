#! /usr/bin/env ruby

require 'drb'
require 'logger/colors'
require 'optparse'
require 'securerandom'
require 'socket'

require_relative 'src/status_checker'
require_relative 'src/worker'
require_relative 'config/config'


# Parsing Arguments zzz....
options = {:periodic=>true} # Default arguments
OptionParser.new do |opts|
  opts.on("--[no-]periodic-checking", "Enable/disable periodic checking. Enables by default.") do |v|
    options[:periodic] = v
  end

  # Specify the port to listen
  opts.on('-p port', '--port port', 'Specify port to use') do |port|
    options[:port] = port.to_i
  end

  # Specify the druby address of the dispatcher
  opts.on('-s dispatcher_address', '--dispatcher dispatcher_address', 'Specify the address of the dispatcher') do |addr|
    options[:dispatcher_addr] = "druby://#{options[:dispatcher_addr]}"
  end

end.parse!

options[:port] ||= (ARGV.shift || CHT_Configuration::Address::DefaultPorts::STATUS_CHECKER_DEFAULT_PORT).to_i
options[:dispatcher_addr] ||= CHT_Configuration::Address::druby_uri(CHT_Configuration::Address::DISPATCHER)

if !ARGV.empty?
  print("Unrecognized arguments: ", *ARGV)
  print("\n")
  exit(false)
end

# Initiate the workers as DRb objects
logger = Logger.new(STDERR)
logger.level = CHT_Configuration::LOGGER_LEVEL
workers = Hash[CHT_Configuration::Address::WORKERS.map{|n,addr| [n, DRbObject.new_with_uri(CHT_Configuration::Address::druby_uri(addr))]}]
dispatcher = DRbObject.new_with_uri options[:dispatcher_addr]
status_checker = StatusChecker.new(workers,
                                   :dispatcher => dispatcher,
                                   :logger=>logger,
                                   :update_period =>(options[:periodic] ?
                                                     CHT_Configuration::STATUS_CHECKER_UPDATE_PERIOD :
                                                     nil)
                                  )

# Run the server
druby_uri = CHT_Configuration::Address::druby_uri(:address => '', :port => options[:port])
DRb.start_service druby_uri, status_checker
$stderr.puts "Running on #{druby_uri}..."
DRb.thread.join
