#! /usr/bin/env ruby


require_relative 'workload_parser.rb'

result = Marshal.load($stdin.read)

finish_time = result[:finish_time]
jobs = result[:jobs]
puts "#{finish_time.select{|j, t| jobs[j].deadline >= t}.size} out of #{jobs.size} jobs met deadline."
