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
    options[:host] ||= CHT_Configuration::Address::WORKERS[name][:address] rescue nil
    options[:port] ||= CHT_Configuration::Address::WORKERS[name][:port] rescue nil
    options[:heterogeneous_factor] ||= CHT_Configuration::WORKER_PARAMETER[name][:heterogeneous_factor] rescue nil
    options[:gpu_factor] ||= CHT_Configuration::WORKER_PARAMETER[name][:gpu_factor] rescue nil
  end

  # Specify the port to listen
  opts.on('-p port', '--port port', 'Specify port to use') do |port|
    options[:port] = port.to_i
  end

  # Specify the status checker address
  opts.on('-s status_checker_address', '--status_checker_address status_checker_address', 'Specify address of status checker') do |addr|
    options[:status_checker_address] = addr
  end

  # Specify the dispatcher address
  opts.on('-d dispatcher_address', '--dispatcher_address dispatcher_address', 'Specify address of dispatcher') do |addr|
    options[:dispatcher_address] = addr
  end

  # TODO: parse the arguments of distribution of actual sleeping time
  msg = 'Specify the argument for simulating heterogeneous environment. \
          This enables heterogeneous simulation'
  opts.on('-a heterogeneous-factor', '--heterogeneous-factor heterogeneous_factor', Float, msg) do |arg|
    options[:heterogeneous_factor] = arg
  end

end.parse!

# TODO: set the default value of the arguments of distribution of actual sleeping time
if options[:gpu_factor] != nil && options[:heterogeneous_factor] == nil
  $stderr.puts("Arguments of heterogeneous simulation not matching.\n")
  p options
  exit(false)
end

options[:host] ||= '127.0.0.1'
options[:port] ||= (ARGV.shift || CHT_Configuration::Address::DefaultPorts::WORKER_DEFAULT_PORT).to_i
options[:name] ||= ARGV.shift || `hostname` || SecureRandom.uuid
options[:status_checker_address] = "druby://#{options[:status_checker_address]}" if options[:status_checker_address]
options[:dispatcher_address] = "druby://#{options[:dispatcher_address]}" if options[:dispatcher_address]


if !ARGV.empty?
  $stderr.print("Unrecognized arguments: ", *ARGV)
  $stderr.print("\n")
  exit(false)
end

# Initiate and run the worker as a DRb object
logger = Logger.new(STDERR)
logger.level = CHT_Configuration::LOGGER_LEVEL
#logger.level = Logger::INFO
status_checker_druby_uri = options[:status_checker_address] || CHT_Configuration::Address.druby_uri(CHT_Configuration::Address::STATUS_CHECKER)
status_checker = DRbObject.new_with_uri status_checker_druby_uri
dispatcher_druby_uri = options[:dispatcher_address] || CHT_Configuration::Address.druby_uri(CHT_Configuration::Address::DISPATCHER)
dispatcher = DRbObject.new_with_uri dispatcher_druby_uri

worker_druby_uri = CHT_Configuration::Address.druby_uri(:address => '', :port => options[:port])
external_worker_uri = CHT_Configuration::Address.druby_uri(:address => options[:host], :port => options[:port])

name = options[:name]
arg = {
  :logger=>logger,
  :status_checker=>status_checker,
  :dispatcher=>dispatcher,
  :uri => external_worker_uri,
}

worker = case
         when options[:gpu_factor]
           SimulatedGPUHeterogeneousWorker.new(name, options[:heterogeneous_factor], options[:gpu_factor], arg)
         when options[:heterogeneous_factor]
           SimulatedHeterogeneousWorker.new(name, options[:heterogeneous_factor], arg)
         else
           Worker.new(name, arg)
         end

DRb.start_service worker_druby_uri, worker
logger.info "Worker #{options[:name]} running on #{worker_druby_uri}..."
logger.info "Worker external uri set to #{external_worker_uri}..."
worker.register # Must registier only after service started
worker.start
DRb.thread.join
