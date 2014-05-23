module WorkloadParser
  def workload_from_file(file)
    lines = file.readlines.map{|l| l.strip}
    jobs = []
    lines.each do |l|
      if l.strip.empty?
        next
      elsif l =~ /=+Batch(.*)=+/
        jobs << {:name => l.match(/[^=]+/).to_s, :task => []}
      else
        jobs[-1][:task] << {
          :download => /download:(.*?) sec./.match(l)[1].to_f,
          :process => /process:(.*?) sec./.match(l)[1].to_f,
          :upload => /upload:(.*?) sec./.match(l)[1].to_f,
        }
      end
    end
    return jobs
  end
  module_function :workload_from_file
end
