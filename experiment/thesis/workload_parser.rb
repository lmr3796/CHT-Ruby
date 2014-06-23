require 'csv'
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
  attr_accessor :sample_rate,:scale_rate,:time_limit
  def initialize(job_set, opt={})
    self.reset
    self.job_set = job_set
    self.sample_rate = opt[:job_sample_rate] if opt.has_key? :job_sample_rate
    self.scale_rate  = opt[:job_scale_rate] if opt.has_key? :job_scale_rate
    self.time_limit  = opt[:job_time_limit] if opt.has_key? :job_time_limit
    self.deadline_rate = opt[:deadline_rate] if opt.has_key? :deadline_rate
  end
  def job_set=(j)
    @job_set = j.clone
  end
  def deadline_rate=(r)
    raise ArgumentError if not r.is_a? Numeric
    @deadline_rate = r
  end
  def sample_rate=(r)
    raise ArgumentError if not r.is_a? Numeric
    @sample_rate = r
  end
  def scale_rate=(r)
    raise ArgumentError if not r.is_a? Numeric
    @scale_rate = r
  end
  def time_limit=(t)
    raise ArgumentError if not t.is_a? Range
    @time_limit = t
  end
  def reset()
    @sample_rate   = 1.0
    @scale_rate    = 1.0
    @time_limit    = nil
    @deadline_rate = nil
  end

  def estimated_cpu_time()
    return job_set_to_run.map{|j|j[:run_time] * j[:allocated_processors]}.reduce(:+) 
  end
  def filter_time_limit(job_set, limit=@time_limit)
    raise 'Jobs not set' if job_set == nil
    return job_set.clone if limit == nil
    return job_set.select {|j| limit.cover? j[:run_time]}
  end
  def sample(job_set, r=@sample_rate)
    return job_set.clone if r == nil
    return job_set.select{rand <= r}
  end
  def scale(job_set, r=@scale_rate)
    return job_set.clone if r == nil
    return job_set.map{|j| j2 = j.clone; j2[:run_time] *= r; j2}
  end
  def run()
    return job_set_to_run
  end
  def job_set_to_run 
    j = @job_set
    j = sample(j)
    j = filter_time_limit(j)
    j = scale(j)
    j = j.each{|i|i[:deadline] = i[:run_time] * @deadline_rate}
    return j
  end
  private :sample, :scale, :filter_time_limit
end
