module ServerStatusChecking
  def alive?
    return true
  end
  def coordination
    return {
      :status_checker => @status_checker.to_s,
      :decision_maker => @decision_maker.to_s,
      :dispatcher => @dispatcher.to_s,
      :worker => @worker_table.to_s,
    }
  end
end
