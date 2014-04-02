#! /usr/bin/env ruby

require 'drb'
require 'optparse'
require 'securerandom'
require 'socket'

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

  # Specify the druby address of the dispatcher
  opts.on('-s dispatcher_address', '--dispatcher dispatcher_address', 'Specify the address of the dispatcher') do |addr|
    options[:dispatcher_addr] = "druby://#{options[:dispatcher_addr]}"
  end

end.parse!

options[:port] ||= (ARGV.shift || CHT_Configuration::Address::DefaultPorts::STATUS_CHECKER_DEFAULT_PORT).to_i
options[:dispatcher_addr] ||= CHT_Configuration::Address::druby_uri(CHT_Configuration::Address::DISPATCHER)

if !ARGV.empty?
  print("Unrecognized arguments: ", *ARGV, "\n")
  exit(false)
end

# Initiate and run the worker as a DRb object
workers = Hash[CHT_Configuration::Address::WORKERS.map{|n,addr| [n, DRbObject.new_with_uri(CHT_Configuration::Address::druby_uri(addr))]}]
dispatcher = DRbObject.new_with_uri options[:dispatcher_addr]
status_checker = StatusChecker.new workers, :dispatcher => dispatcher

druby_uri = CHT_Configuration::Address::druby_uri(:address => '', :port => options[:port])
DRb.start_service druby_uri, status_checker
$stderr.puts "Running on #{druby_uri}..."
DRb.thread.join
