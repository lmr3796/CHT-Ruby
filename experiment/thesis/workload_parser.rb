require 'csv'

require_relative '../../config/config'
require_relative '../../src/client'
require_relative '../../src/job'

module StandardWorkloadFormatParser
  FIELDS = [
    :job_number,
    :submit_time,
    :wait_time,
    :run_time,
    :allocated_processors,
    :average_cpu_time,
    :used_memory,
    :requested_processors,
    :requested_time,
    :requested_memory,
    :status,
    :user_id,
    :group_id,
    :executable_number,
    :queue_number,
    :partition_number,
    :preceding_job_number,
    :think_time_from_preceding,
  ]
  def from_file(f)
    return f.map{|l| from_line l.strip}
  end
  def from_line(l)
    return Hash[FIELDS.zip(l.split.map{|s|s.to_i})].select{|k,v| v!=-1}
  end
  module_function :from_line, :from_file
end

class WorkloadSynthesizer
  attr_accessor :sample_rate,:wait_time_scale_rate,
    :job_scale_rate,:exec_time_limit, :wait_time_limit
  def initialize(job_set, opt={})
    self.reset
    self.job_set = job_set
    self.sample_rate = opt[:job_sample_rate] if opt.has_key? :job_sample_rate
    self.job_scale_rate  = opt[:job_scale_rate] if opt.has_key? :job_scale_rate
    self.wait_time_scale_rate  = opt[:job_scale_rate] if opt.has_key? :wait_time_scale_rate
    self.exec_time_limit  = opt[:job_exec_time_limit] if opt.has_key? :job_exec_time_limit
    self.wait_time_limit  = opt[:job_wait_time_limit] if opt.has_key? :job_wait_time_limit
    self.deadline_rate = opt[:deadline_rate] if opt.has_key? :deadline_rate
  end
  def job_set=(j)
    @job_set = Marshal.load(Marshal.dump(j)) 
  end
  def deadline_rate=(r)
    raise ArgumentError if not r.is_a? Numeric
    @deadline_rate = r
  end
  def sample_rate=(r)
    raise ArgumentError if not r.is_a? Numeric
    @sample_rate = r
  end
  def job_scale_rate=(r)
    raise ArgumentError if not r.is_a? Numeric
    @job_scale_rate = r
  end
  def wait_time_scale_rate=(r)
    raise ArgumentError if not r.is_a? Numeric
    @wait_time_scale_rate = r
  end
  def exec_time_limit=(t)
    raise ArgumentError if not t.is_a? Range
    @exec_time_limit = t
  end
  def wait_time_limit=(t)
    raise ArgumentError if not t.is_a? Numeric
    @wait_time_limit = t
  end
  def reset()
    @sample_rate          = 1.0
    @job_scale_rate       = 1.0
    @wait_time_scale_rate = 1.0
    @exec_time_limit      = nil
    @wait_time_limit      = nil
    @deadline_rate        = nil
  end

  def estimated_cpu_time()
    return job_set_to_run.map{|j|j[:run_time] * j[:allocated_processors]}.reduce(:+) 
  end
  def filter_exec_time_limit(job_set)
    raise 'Jobs not set' if job_set == nil
    return Marshal.load(Marshal.dump(job_set)) if @exec_time_limit == nil
    return job_set.select {|j| @exec_time_limit.cover? j[:run_time]}
  end
  def filter_wait_time_limit(job_set)
    raise 'Jobs not set' if job_set == nil
    return Marshal.load(Marshal.dump(job_set)) if @wait_time_limit == nil
    return job_set.select {|j| j[:submit_time] - job_set[0][:submit_time] <= @wait_time_limit}
  end
  def sample(job_set)
    j = Marshal.load(Marshal.dump(job_set))
    window_size = (j.size * @sample_rate).to_i
    window_begin = rand(0..(j.size-window_size))
    return j[window_begin...(window_begin + window_size)]
  end
  def scale(job_set)
    job_set = Marshal.load(Marshal.dump(job_set))
    job_set.each do |j|
      j[:run_time] *= @job_scale_rate if @job_scale_rate != nil
      j[:submit_time] *= @wait_time_scale_rate if @wait_time_scale_rate != nil
    end
    return job_set
  end

  def run()
    job_set = job_set_to_run
    # Parse priority by user
    group = Hash.new(0)
    job_set.each{|j|group[j[:user_id]] += 1}

    job_set = job_set.map do |j|
      job = Job.new
      # Model priority by user
      job.priority = group[j[:user_id]]
      (0...j[:allocated_processors]).each do
        job.add_task Task.new("sleep #{j[:run_time]}")
      end
      # Parse submission time
      [job, j[:submit_time]-job_set[0][:submit_time]]
    end
    #p job_set
  end
  
  def job_set_to_run 
    j = Marshal.load(Marshal.dump(@job_set))
    j = filter_wait_time_limit(j)
    j = filter_exec_time_limit(j)
    j = sample(j)
    #p "jizz: #{j.size}"
    j = scale(j)
    j = j.each{|i|i[:deadline] = i[:run_time] * @deadline_rate}
    return j
  end
  private :sample, :scale, :filter_exec_time_limit
end
