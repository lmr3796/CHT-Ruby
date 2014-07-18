#! /usr/bin/env ruby

require 'drb'
require 'logger/colors'
require 'optparse'
require 'securerandom'
require 'socket'

require_relative 'src/decision_maker'
require_relative 'config/config'


# Parsing Arguments zzz....
options = {}
OptionParser.new do |opts|

  # Specify the port to listen
  opts.on('-p port', '--port port', 'Specify port to use') do |port|
    options[:port] = port.to_i
  end

  # Specify the algorithm to use
  opts.on('-a algorithm_name', '--algorithm algorithm_name', 'Specify the scheduling algorithm') do |algo|
    $stderr.puts "This options is currently unsupported"
    exit(false)
    # TODO: specify by name
    options[:algo] = algo
  end

  # Specify the druby address of the status checker
  opts.on('-s status_checker_address', '--status-checker status_checker_address', 'Specify the address of the status checker') do |addr|
    options[:status_checker] = "druby://#{options[:status_checker]}"
  end

end.parse!

options[:port] ||= (ARGV.shift || CHT_Configuration::Address::DefaultPorts::DECISION_MAKER_DEFAULT_PORT).to_i
options[:status_checker] ||= CHT_Configuration::Address::druby_uri(CHT_Configuration::Address::STATUS_CHECKER)
options[:algo] ||= CHT_Configuration::Algorithm::ALGORITHM

if !ARGV.empty?
  print("Unrecognized arguments: ", *ARGV)
  print("\n")
  exit(false)
end

# Initiate and run the decision maker as a DRb object
logger = Logger.new(STDERR)
logger.level = CHT_Configuration::LOGGER_LEVEL
algorithm = options[:algo].new
status_checker = DRbObject.new_with_uri options[:status_checker]
decision_maker = DecisionMaker.new algorithm, status_checker, :logger=>logger

druby_uri = CHT_Configuration::Address::druby_uri(:address => '', :port => options[:port])
DRb.start_service druby_uri, decision_maker
logger.info "Running on #{druby_uri}..."
DRb.thread.join
