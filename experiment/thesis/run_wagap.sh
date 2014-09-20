D_MAKER_ADDR='druby://localhost:10003'

echo 'Running Priority'
../../algorithm_changer.rb $D_MAKER_ADDR PriorityBasedScheduling 
./run_exp.sh wagap-d5-b1-c0_001-w0_1-s0_01-Priority/wagap.sample.?

echo 'Running EDF'
../../algorithm_changer.rb $D_MAKER_ADDR EarliestDeadlineFirstScheduling 
./run_exp.sh wagap-d5-b1-c0_001-w0_1-s0_01-EarliestDeadlineFirst/wagap.sample.?

echo 'Running DeadlineNoPriority'
../../algorithm_changer.rb $D_MAKER_ADDR PreemptiveNoPriorityDeadlineBasedScheduling 
./run_exp.sh wagap-d5-b1-c0_001-w0_1-s0_01-DeadlineNoPriority/wagap.sample.?

echo 'Running DeadlinePriority'
../../algorithm_changer.rb $D_MAKER_ADDR PreemptiveDeadlineBasedScheduling 
./run_exp.sh wagap-d5-b1-c0_001-w0_1-s0_01-DeadlinePriority/wagap.sample.?
