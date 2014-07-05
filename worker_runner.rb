#! /usr/bin/env ruby

require 'drb'
require 'logger/colors'
require 'optparse'
require 'securerandom'
require 'socket'

require_relative 'src/worker'
require_relative 'config/config'


# Parsing Arguments zzz....
options = {}
OptionParser.new do |opts|

  # Specify the server name, may co
  opts.on('-n name', '--name name', 'Specify server name. \
          If the name exists in the configuration will use corresponding settings') do |name|
    options[:name] = name
    options[:port] ||= CHT_Configuration::Address::WORKERS[name][:port] rescue nil
  end

  # Specify the port to listen
  opts.on('-p port', '--port port', 'Specify port to use') do |port|
    options[:port] = port.to_i
  end

  # Specify the status checker address
  opts.on('-s status_checker_address', '--status_checker_address status_checker_address', 'Specify address of status checker') do |addr|
    options[:status_checker_address] = addr
  end

  # Specify the status checker address
  opts.on('-d dispatcher_address', '--dispatcher_address dispatcher_address', 'Specify address of dispatcher') do |addr|
    options[:dispatcher_address] = addr
  end

end.parse!

options[:port] ||= (ARGV.shift || CHT_Configuration::Address::DefaultPorts::WORKER_DEFAULT_PORT).to_i
options[:name] ||= ARGV.shift || `hostname` || SecureRandom.uuid
options[:status_checker_address] = "druby://#{options[:status_checker_address]}" if options[:status_checker_address]
options[:dispatcher_address] = "druby://#{options[:dispatcher_address]}" if options[:dispatcher_address]


if !ARGV.empty?
  print("Unrecognized arguments: ", *ARGV)
  print("\n")
  exit(false)
end

# Initiate and run the worker as a DRb object
logger = Logger.new(STDERR)
logger.level = CHT_Configuration::LOGGER_LEVEL
status_checker_druby_uri = options[:status_checker_address] || CHT_Configuration::Address.druby_uri(CHT_Configuration::Address::STATUS_CHECKER)
status_checker = DRbObject.new_with_uri status_checker_druby_uri
dispatcher_druby_uri = options[:dispatcher_address] || CHT_Configuration::Address.druby_uri(CHT_Configuration::Address::DISPATCHER)
dispatcher = DRbObject.new_with_uri dispatcher_druby_uri

worker = Worker.new(options[:name],
                    :logger=>logger,
                    :status_checker=>status_checker,
                    :dispatcher=>dispatcher)
worker_druby_uri = CHT_Configuration::Address.druby_uri(:address => '', :port => options[:port])
DRb.start_service worker_druby_uri, worker
$stderr.puts "Worker #{options[:name]} running on #{worker_druby_uri}..."
begin
  worker.register # Must registier only after service started
rescue DRb::DRbConnError
  logger.error "Error reaching the management system, waiting for retry"
  sleep 3
  retry
end
worker.start
DRb.thread.join
