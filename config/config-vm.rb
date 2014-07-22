require 'logger/colors'

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

    DISPATCHER      = {:address => '192.168.10.211', :port  => DefaultPorts::DISPATCHER_DEFAULT_PORT}
    STATUS_CHECKER  = {:address => '192.168.10.115', :port  => DefaultPorts::STATUS_CHECKER_DEFAULT_PORT}
    DECISION_MAKER  = {:address => '192.168.10.47', :port  => DefaultPorts::DECISION_MAKER_DEFAULT_PORT}

    WORKERS = {
	'cht04-1' => {:address => '192.168.10.247', :port => 20001},
	'cht04-2' => {:address => '192.168.10.247', :port => 20002},
	'cht05-1' => {:address => '192.168.10.182', :port => 20001},
	'cht05-2' => {:address => '192.168.10.182', :port => 20002},
	'cht06-1' => {:address => '192.168.10.23' , :port => 20001},
	'cht06-2' => {:address => '192.168.10.23' , :port => 20002},
	'cht07-1' => {:address => '192.168.10.96' , :port => 20001},
	'cht07-2' => {:address => '192.168.10.96' , :port => 20002},
	'cht08-1' => {:address => '192.168.10.149', :port => 20001},
	'cht08-2' => {:address => '192.168.10.149', :port => 20002},
	'cht09-1' => {:address => '192.168.10.181', :port => 20001},
	'cht09-2' => {:address => '192.168.10.181', :port => 20002},
	'cht10-1' => {:address => '192.168.10.191', :port => 20001},
	'cht10-2' => {:address => '192.168.10.191', :port => 20002},
	'cht11-1' => {:address => '192.168.10.120', :port => 20001},
	'cht11-2' => {:address => '192.168.10.120', :port => 20002},
	'cht12-1' => {:address => '192.168.10.183', :port => 20001},
	'cht12-2' => {:address => '192.168.10.183', :port => 20002},
	'cht13-1' => {:address => '192.168.10.137', :port => 20001},
	'cht13-2' => {:address => '192.168.10.137', :port => 20002},
    }

    def self.druby_uri(socket)
      return "druby://#{socket[:address]}:#{socket[:port]}"
    end
  end

  STATUS_CHECKER_UPDATE_PERIOD = 10 #seconds
  LOGGER_LEVEL = Logger::INFO # Logger::[INFO/WARN/DEBUG/ERROR/FATAL/UNKNOWN]
end
