#! /usr/bin/env ruby
require 'drb'
require_relative 'src/decision_maker'

Dir[File.expand_path("src/algorithms/*.rb", File.dirname(__FILE__))].each{|f| require f}
include SchedulingAlgorithm

decision_maker = DRbObject.new_with_uri ARGV[0]
decision_maker.algorithm = Object.const_get(ARGV[1]).new
