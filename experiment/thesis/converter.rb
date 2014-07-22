#! /usr/bin/env ruby

require_relative '../../src/job.rb'
require_relative './workload_parser.rb'

ARGV.each do |f|
  f = open(f, 'r')
  batch = Marshal.load(f.read) 
  f.close
  batch.each do |b|
    b[:batch].each do |j|
      j.instance_variable_get('@progress').update{|q|Job::Progress.new(q,0,0)}
    end
  end
  open(f,'w').write Marshal.dump(batch)
end
