module CHT_Configuration
  # Specifying algorithms
  module Algorithm
    # Importing built-in algorithms
    Dir[File.expand_path("../src/algorithms/*.rb", File.dirname(__FILE__))].each{|f| require f}
    include SchedulingAlgorithm
    # require 'path/to/your/algorithm-without-extension-name'
    # ALGORITHM = YourModule::YourAlgorithm
    ALGORITHM = DeadlineBasedScheduling  #Just Provide class name
  end
  module Address
    module DefaultPorts
      WORKER_DEFAULT_PORT          = 10000
      DISPATCHER_DEFAULT_PORT      = 10001
      STATUS_CHECKER_DEFAULT_PORT  = 10002
      DECISION_MAKER_DEFAULT_PORT  = 10003
    end

    DISPATCHER      = {:address => '192.168.10.112', :port  => DefaultPorts::DISPATCHER_DEFAULT_PORT}
    STATUS_CHECKER  = {:address => '192.168.10.112', :port  => DefaultPorts::STATUS_CHECKER_DEFAULT_PORT}
    DECISION_MAKER  = {:address => '192.168.10.112', :port  => DefaultPorts::DECISION_MAKER_DEFAULT_PORT}

    WORKERS = {
      'lmr3796-124-2' => {:address => '192.168.10.243', :port => DefaultPorts::WORKER_DEFAULT_PORT},
      'lmr3796-124-3' => {:address => '192.168.10.123', :port => DefaultPorts::WORKER_DEFAULT_PORT},
      'lmr3796-124-4' => {:address => '192.168.10.229', :port => DefaultPorts::WORKER_DEFAULT_PORT},
      'lmr3796-124-5' => {:address => '192.168.10.18',  :port => DefaultPorts::WORKER_DEFAULT_PORT},
    }

    def self.druby_uri(socket)
      return "druby://#{socket[:address]}:#{socket[:port]}"
    end
  end
end
