require_relative 'workload_parser.rb'

class WorkloadSynthesizer
  attr_accessor :sample_rate,:scale_rate,:time_limit
  def initialize(jobs, opt={})
    @sample_rate = 1.0
    @scale_rate  = 1.0
    @time_limit  = nil
    self.jobs = jobs
    self.sample_rate = opt[:job_sample_rate] if opt.has_key? :job_sample_rate
    self.scale_rate  = opt[:job_scale_rate] if opt.has_key? :job_scale_rate
    self.time_limit  = opt[:job_time_limit] if opt.has_key? :job_time_limit
  end
  def jobs=(j)
    @original_job = j.clone
    @jobs_to_run = j.clone
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
    raise ArgumentError if not t.is_a? Integer
    @time_limit = t
  end
  def reset()
    @job_to_run = @original_job
  end
  def filter_time_limit(limit=@time_limit)
    raise 'Jobs not set' if @original_job == nil
    @job_to_run = @job_to_run.select {|j|j[:run_time] < limit}
  end
  def sample(r=@sample_rate)
    sample_size = (@job_to_run.size*r).to_i
    @job_to_run = @job_to_run.sample(sample_size)
  end
  def run()

  end
end
