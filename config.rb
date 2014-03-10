module CHT

  module Config
    DISPATCHER={:address => '192.168.10.112', :port  => 10000}
    WORKER={
      'cht2' => {:address => '192.168.10.243',    :port => 10001},
      'cht3' => {:address => '192.168.10.133',    :port => 10002},
      'cht4' => {:address => '192.168.10.229',    :port => 10003},
      'cht5' => {:address => '192.168.10.18',     :port => 10004}
    }

    def self.druby_uri(socket)
      return "druby://#{socket[:address]}:#{socket[:port]}"
    end
  end

end
