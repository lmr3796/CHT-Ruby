require 'csv'
require 'logger/colors'

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
    return f.reject{|l| l[0] == ';'}.map{|l| from_line l.strip}
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
    @logger = opt[:logger]
    self.reset
    self.job_set = job_set
    self.sample_rate = opt[:job_sample_rate] if opt.has_key? :job_sample_rate
    self.job_scale_rate = opt[:job_scale_rate] if opt.has_key? :job_scale_rate
    self.wait_time_scale_rate = opt[:wait_time_scale_rate] if opt.has_key? :wait_time_scale_rate
    self.batch_threshold = opt[:batch_threshold] if opt.has_key? :batch_threshold
    self.exec_time_limit = opt[:job_exec_time_limit] if opt.has_key? :job_exec_time_limit
    self.wait_time_limit = opt[:job_wait_time_limit] if opt.has_key? :job_wait_time_limit
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
  def batch_threshold=(t)
    raise ArgumentError if not t.is_a? Numeric
    raise ArgumentError if t < 0
    @batch_threshold = t
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
  def shift_submit_time(job_set)
    raise 'Jobs not set' if job_set == nil
    job_set = Marshal.load(Marshal.dump(job_set))
    return [] if job_set.empty?
    base_time = job_set[0][:submit_time]
    job_set.each do |j|
      j[:submit_time] -= base_time
    end
    return job_set
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

  # Synthesize from workload
  def job_set_to_run
    j = Marshal.load(Marshal.dump(@job_set))
    return [] if j == nil
    j = sample(j)
    j = filter_exec_time_limit(j)
    j = shift_submit_time(j)
    j = filter_wait_time_limit(j)
    j = scale(j)
    j = j.each do |i|
      i[:deadline] = i[:run_time] * @deadline_rate
      i[:deadline] *= (i[:allocated_processors]/4.0).ceil if i[:allocated_processors] > 4
    end
    return j
  end

  # Convert to our format
  # Batch is generated here
  def gen_workload()
    job_set = job_set_to_run
    return [] if job_set.empty?
    # Parse priority by user
    group = Hash.new(0)
    job_set.each{|j|group[j[:user_id]] += 1}

    job_set = job_set.map do |j|
      job = Job.new
      # Use execution time as deadline first, convert it on simulation
      job.deadline = Time.at(j[:deadline])

      # Model priority by user
      job.priority = group[j[:user_id]]
      (0...j[:allocated_processors]).each do
        job.add_task SleepTask.new(j[:run_time].to_f)
      end
      # Parse submission time
      {:job => job, :submit_time => j[:submit_time]}
    end

    # Parse submission time to wait (sleep) time
    (0...job_set.size-1).each do |i|
      job_set[i][:wait_time] = job_set[i+1][:submit_time] - job_set[i][:submit_time]
    end
    job_set[-1][:wait_time] = 0
    job_set.each{|j| j.delete :submit_time}

    # Merge batch
    merged_batch = [{:wait_time => 0, :batch =>[]}]
    job_set.each do |j|
      if j[:wait_time] > @batch_threshold or merged_batch[-1][:wait_time] > @batch_threshold
        merged_batch << {:wait_time => j[:wait_time], :batch =>[j[:job]]}
      else
        merged_batch[-1][:wait_time] += j[:wait_time]
        merged_batch[-1][:batch] << j[:job]
      end
    end
    merged_batch.shift if merged_batch[0][:batch].empty?

    # Generate batch deadline
    merged_batch.each do |b|
      batch_deadline = b[:batch].map{|j| j.deadline.to_f}.max * @deadline_rate
      b[:batch].each{|j| j.deadline = Time.at(batch_deadline)}
    end

    return merged_batch
  end

  private :sample, :scale, :filter_exec_time_limit
end
