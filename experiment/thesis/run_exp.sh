#! /bin/bash
set -e
echo '' > current.log
for l in ../../*.log; do
	> $l
done
for f in $@; do
	log_dir=logs/$f/
	mkdir -p $log_dir
	echo "Running $f" >> current.log
	(time ./workload_executer.rb -i $f -o ${f}.out 2>client.log) >> ${f}.res 2>&1 
	cp ../../decision_maker.log $log_dir
	sync
	echo '' > ../../decision_maker.log
	sync
done
