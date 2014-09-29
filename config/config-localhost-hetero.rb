require 'logger/colors'

module CHT_Configuration
  # Specifying algorithms
  module Algorithm
    # Importing built-in algorithms
    Dir[File.expand_path("../src/algorithms/*.rb", File.dirname(__FILE__))].each{|f| require f}
    include SchedulingAlgorithm
    # require 'path/to/your/algorithm-without-extension-name'
    # ALGORITHM = YourModule::YourAlgorithm
    ALGORITHM = PriorityBasedScheduling  #Just Provide class name
    #ALGORITHM = DeadlineBasedScheduling  #Just Provide class name
    #ALGORITHM = PreemptiveDeadlineBasedScheduling  #Just Provide class name
    #ALGORITHM = PreemptiveNoPriorityDeadlineBasedScheduling  #Just Provide class name
    #ALGORITHM = EarliestDeadlineFirstScheduling  #Just Provide class name
  end
  module Address
    module DefaultPorts
      WORKER_DEFAULT_PORT          = 20000
      DISPATCHER_DEFAULT_PORT      = 10001
      STATUS_CHECKER_DEFAULT_PORT  = 10002
      DECISION_MAKER_DEFAULT_PORT  = 10003
    end

    DISPATCHER      = {:address => 'localhost', :port  => DefaultPorts::DISPATCHER_DEFAULT_PORT}
    STATUS_CHECKER  = {:address => 'localhost', :port  => DefaultPorts::STATUS_CHECKER_DEFAULT_PORT}
    DECISION_MAKER  = {:address => 'localhost', :port  => DefaultPorts::DECISION_MAKER_DEFAULT_PORT}

    WORKERS = {
      #'linux8-1' => {:address => 'linux8.csie.org', :port => 20001},
      #'linux8-2' => {:address => 'linux8.csie.org', :port => 20002},
      #'linux8-3' => {:address => 'linux8.csie.org', :port => 20003},
      #'linux8-4' => {:address => 'linux8.csie.org', :port => 20004},
      #'linux9-1' => {:address => 'linux9.csie.org', :port => 20001},
      #'linux9-2' => {:address => 'linux9.csie.org', :port => 20002},
      #'linux9-3' => {:address => 'linux9.csie.org', :port => 20003},
      #'linux9-4' => {:address => 'linux9.csie.org', :port => 20004},
      #'linux14-1' => {:address => 'linux14.csie.org', :port => 20001},
      #'linux14-2' => {:address => 'linux14.csie.org', :port => 20002},
      #'linux14-3' => {:address => 'linux14.csie.org', :port => 20003},
      #'linux14-4' => {:address => 'linux14.csie.org', :port => 20004},
      'localhost1'  => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT},
      'localhost2'  => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+2},
      'localhost3'  => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+3},
      'localhost4'  => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+4},
      'localhost5'  => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+5},
      'localhost6'  => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+6},
      'localhost7'  => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+7},
      'localhost8'  => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+8},
      'localhost9'  => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+9},
      'localhost10' => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+10},
      'localhost11' => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+11},
      'localhost12' => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+12},
      'localhost13' => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+13},
      'localhost14' => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+14},
      'localhost15' => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+15},
      'localhost16' => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+16},
      'localhost17' => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+17},
      'localhost18' => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+18},
      'localhost19' => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+19},
      'localhost20' => {:address => '127.0.0.1', :port => DefaultPorts::WORKER_DEFAULT_PORT+20},
      #'lmr3796-124-2' => {:address => '192.168.10.243', :port => DefaultPorts::WORKER_DEFAULT_PORT},
      #'lmr3796-124-3' => {:address => '192.168.10.123', :port => DefaultPorts::WORKER_DEFAULT_PORT},
      #'lmr3796-124-4' => {:address => '192.168.10.229', :port => DefaultPorts::WORKER_DEFAULT_PORT},
      #'lmr3796-124-5' => {:address => '192.168.10.18',  :port => DefaultPorts::WORKER_DEFAULT_PORT},
    }

    def self.druby_uri(socket)
      return "druby://#{socket[:address]}:#{socket[:port]}"
    end
  end

  WORKER_PARAMETER = {
    'localhost1'  => {:heterogeneous_factor => 0.5},
    'localhost2'  => {:heterogeneous_factor => 0.5},
    'localhost3'  => {:heterogeneous_factor => 0.5},
    'localhost4'  => {:heterogeneous_factor => 0.5},
    'localhost5'  => {:heterogeneous_factor => 0.8, :gpu_factor => 0.1},
    'localhost6'  => {:heterogeneous_factor => 0.8, :gpu_factor => 0.1},
    'localhost7'  => {:heterogeneous_factor => 0.8, :gpu_factor => 0.1},
    'localhost8'  => {:heterogeneous_factor => 0.8, :gpu_factor => 0.1},
    'localhost9'  => {:heterogeneous_factor => 1.0},
    'localhost10' => {:heterogeneous_factor => 1.0},
    'localhost11' => {:heterogeneous_factor => 1.0},
    'localhost12' => {:heterogeneous_factor => 1.0},
    'localhost13' => {:heterogeneous_factor => 1.2},
    'localhost14' => {:heterogeneous_factor => 1.2},
    'localhost15' => {:heterogeneous_factor => 1.2},
    'localhost16' => {:heterogeneous_factor => 1.2},
    'localhost17' => {:heterogeneous_factor => 1.5},
    'localhost18' => {:heterogeneous_factor => 1.5},
    'localhost19' => {:heterogeneous_factor => 1.5},
    'localhost20' => {:heterogeneous_factor => 1.5},
  }

  STATUS_CHECKER_UPDATE_PERIOD = 10 #seconds
  LOGGER_LEVEL = Logger::DEBUG # Logger::[INFO/WARN/DEBUG/ERROR/FATAL/UNKNOWN]
end
