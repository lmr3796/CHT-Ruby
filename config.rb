#! /usr/bin/env ruby

dispatcher={:address => '192.168.10.112', :port  => 10000}
worker={
    'cht2' => {:address => '192.168.10.243',    :port => 10001},
    'cht3' => {:address => '192.168.10.133',    :port => 10002},
    'cht4' => {:address => '192.168.10.229',    :port => 10003},
    'cht5' => {:address => '192.168.10.18',     :port => 10004}
}
