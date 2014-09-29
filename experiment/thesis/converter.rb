#! /usr/bin/env ruby

require_relative 'workload_parser.rb'

ARGV.each do |f|
  f = open(f)
  batch = Marshal.load(f.read)
  f.close
  batch.each do |b|
    b[:batch].each do |j|
      j.task.each_with_index do |t, i|
        new_task = SleepTask.new(t.args[0].to_f)
        new_task.id = t.id
        j.task[i] = new_task
      end
    end
  end
  f = open(f,'w')
  f.write(Marshal.dump(batch))
end
