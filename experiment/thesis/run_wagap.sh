#! /bin/bash
time ./workload_runner.rb \
-i wagap-d5-b1-c0_001-w0_1-s0_01/wagap.sample.5 \
-o wagap-d5-b1-c0_001-w0_1-s0_01/wagap.sample.5.out \
&& killall ruby
