#! /usr/bin/env ruby

require 'drb'
require 'optparse'
require 'securerandom'
require 'socket'

require_relative 'src/dispatcher'
require_relative 'src/decision_maker'
require_relative 'src/status_checker'
require_relative 'src/worker'
require_relative 'config/config'


# Parsing Arguments zzz....
options = {}
OptionParser.new do |opts|

  # Specify the port to listen
  opts.on('-p port', '--port port', 'Specify port to use') do |port|
    options[:port] = port.to_i
  end

  # Specify the druby address of the status checker
  opts.on('-s status_checker_address', '--status-checker status_checker_address', 'Specify the address of the status checker') do |addr|
    options[:status_checker] = "druby://#{options[:status_checker]}"
  end

  # Specify the druby address of the decision maker
  opts.on('-d decision_maker_address', '--decision-maker decision_maker_address', 'Specify the address of the decision maker') do |addr|
    options[:status_checker] = "druby://#{options[:status_checker]}"
  end

end.parse!

options[:port] ||= (ARGV.shift or CHT_Configuration::Address::DefaultPorts::DISPATCHER_DEFAULT_PORT).to_i
options[:status_checker] ||= CHT_Configuration::Address::druby_uri(CHT_Configuration::Address::STATUS_CHECKER)
options[:decision_maker] ||= CHT_Configuration::Address::druby_uri(CHT_Configuration::Address::DECISION_MAKER)

if !ARGV.empty?
  print("Unrecognized arguments: ", *ARGV, "\n")
  exit(false)
end

# Initiate and run the worker as a DRb object
status_checker = DRbObject.new_with_uri options[:status_checker]
decision_maker = DRbObject.new_with_uri options[:decision_maker]
dispatcher = Dispatcher.new :status_checker => status_checker, :decision_maker => decision_maker

druby_uri = CHT_Configuration::Address::druby_uri(:address => '', :port => options[:port])
DRb.start_service druby_uri, dispatcher 
$stderr.puts "Running on #{druby_uri}..."
DRb.thread.join
