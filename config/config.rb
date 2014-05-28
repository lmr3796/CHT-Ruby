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

    DISPATCHER      = {:address => 'linux11.csie.org', :port  => DefaultPorts::DISPATCHER_DEFAULT_PORT}
    STATUS_CHECKER  = {:address => 'linux11.csie.org', :port  => DefaultPorts::STATUS_CHECKER_DEFAULT_PORT}
    DECISION_MAKER  = {:address => 'linux11.csie.org', :port  => DefaultPorts::DECISION_MAKER_DEFAULT_PORT}

    WORKERS = {
      'linux8-1' => {:address => 'linux8.csie.org', :port => 20001},
      'linux8-2' => {:address => 'linux8.csie.org', :port => 20002},
      'linux8-3' => {:address => 'linux8.csie.org', :port => 20003},
      'linux8-4' => {:address => 'linux8.csie.org', :port => 20004},
      'linux9-1' => {:address => 'linux9.csie.org', :port => 20001},
      'linux9-2' => {:address => 'linux9.csie.org', :port => 20002},
      'linux9-3' => {:address => 'linux9.csie.org', :port => 20003},
      'linux9-4' => {:address => 'linux9.csie.org', :port => 20004},
      'linux14-1' => {:address => 'linux14.csie.org', :port => 20001},
      'linux14-2' => {:address => 'linux14.csie.org', :port => 20002},
      'linux14-3' => {:address => 'linux14.csie.org', :port => 20003},
      'linux14-4' => {:address => 'linux14.csie.org', :port => 20004},
      #'localhost1' => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT},
      #'localhost2' => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+10000},
      #'lmr3796-124-2' => {:address => '192.168.10.243', :port => DefaultPorts::WORKER_DEFAULT_PORT},
      #'lmr3796-124-3' => {:address => '192.168.10.123', :port => DefaultPorts::WORKER_DEFAULT_PORT},
      #'lmr3796-124-4' => {:address => '192.168.10.229', :port => DefaultPorts::WORKER_DEFAULT_PORT},
      #'lmr3796-124-5' => {:address => '192.168.10.18',  :port => DefaultPorts::WORKER_DEFAULT_PORT},
    }

    def self.druby_uri(socket)
      return "druby://#{socket[:address]}:#{socket[:port]}"
    end
  end

  STATUS_CHECKER_UPDATE_PERIOD = 10 #seconds
end
