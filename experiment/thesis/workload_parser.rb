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
