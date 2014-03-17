module CHT_Configuration
  module Address
    DISPATCHER={:address => '192.168.10.112', :port  => 10000}
    WORKER_DEFAULT_PORT = 10001
    WORKERS={
      'lmr3796-124-2' => {:address => '192.168.10.243',    :port => WORKER_DEFAULT_PORT},
      'lmr3796-124-3' => {:address => '192.168.10.123',    :port => WORKER_DEFAULT_PORT},
      'lmr3796-124-4' => {:address => '192.168.10.229',    :port => WORKER_DEFAULT_PORT},
      'lmr3796-124-5' => {:address => '192.168.10.18',     :port => WORKER_DEFAULT_PORT}
    }

    def self.druby_uri(socket)
      return "druby://#{socket[:address]}:#{socket[:port]}"
    end
  end
end
