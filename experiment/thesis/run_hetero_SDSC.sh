#! /usr/bin/env bash
D_MAKER_ADDR='druby://localhost:10003'

echo 'Running Priority'
../../algorithm_changer.rb $D_MAKER_ADDR PriorityBasedScheduling 
./run_exp.sh SDSC-d5-b1-c0_001-w0_1-s0_01-hetero-Priority/SDSC.sample.?

echo 'Running EDF'
../../algorithm_changer.rb $D_MAKER_ADDR EarliestDeadlineFirstScheduling 
./run_exp.sh SDSC-d5-b1-c0_001-w0_1-s0_01-hetero-EarliestDeadlineFirst/SDSC.sample.?

echo 'Running DeadlineNoPriority'
../../algorithm_changer.rb $D_MAKER_ADDR PreemptiveNoPriorityDeadlineBasedScheduling 
./run_exp.sh SDSC-d5-b1-c0_001-w0_1-s0_01-hetero-DeadlineNoPriority/SDSC.sample.?

echo 'Running DeadlinePriority'
../../algorithm_changer.rb $D_MAKER_ADDR PreemptiveDeadlineBasedScheduling 
./run_exp.sh SDSC-d5-b1-c0_001-w0_1-s0_01-hetero-DeadlinePriority/SDSC.sample.?

sleep 3
killall ruby
