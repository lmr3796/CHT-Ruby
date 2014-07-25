#! /usr/bin/env ruby

require_relative 'workload_parser.rb'

ARGV.each do |f|
  f = open(f)
  batch = Marshal.load(f.read)
  f.close
  batch.each do |b|
    b[:batch].each do |j|
      j.priority = rand(0...1000.0)
    end
  end
  f = open(f,'w')
  f.write(Marshal.dump(batch))
end
