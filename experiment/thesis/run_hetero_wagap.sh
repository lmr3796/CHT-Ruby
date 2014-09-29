D_MAKER_ADDR='druby://localhost:10003'

echo 'Running hetero Priority'
../../algorithm_changer.rb $D_MAKER_ADDR PriorityBasedScheduling 
./run_exp.sh wagap-d5-b1-c0_0001-w0_1-s0_01-hetero-Priority/wagap.sample.?

echo 'Running hetero EDF'
../../algorithm_changer.rb $D_MAKER_ADDR EarliestDeadlineFirstScheduling 
./run_exp.sh wagap-d5-b1-c0_0001-w0_1-s0_01-hetero-EarliestDeadlineFirst/wagap.sample.?

echo 'Running hetero DeadlineNoPriority'
../../algorithm_changer.rb $D_MAKER_ADDR PreemptiveNoPriorityDeadlineBasedScheduling 
./run_exp.sh wagap-d5-b1-c0_0001-w0_1-s0_01-hetero-DeadlineNoPriority/wagap.sample.?

echo 'Running hetero DeadlinePriority'
../../algorithm_changer.rb $D_MAKER_ADDR PreemptiveDeadlineBasedScheduling 
./run_exp.sh wagap-d5-b1-c0_0001-w0_1-s0_01-hetero-DeadlinePriority/wagap.sample.?
