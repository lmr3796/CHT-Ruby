module CHT_Configuration
  module Address
    DISPATCHER={:address => '192.168.10.112', :port  => 10000}
    WORKER={
      'cht2' => {:address => '192.168.10.243',    :port => 10001},
      'cht3' => {:address => '192.168.10.133',    :port => 10001},
      'cht4' => {:address => '192.168.10.229',    :port => 10001},
      'cht5' => {:address => '192.168.10.18',     :port => 10001}
    }

    def self.druby_uri(socket)
      return "druby://#{socket[:address]}:#{socket[:port]}"
    end
  end
end
