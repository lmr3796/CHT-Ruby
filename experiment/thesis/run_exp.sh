#! /bin/bash
set -e
for f in $@; do
	echo "Running $f"
	(time ./workload_runner.rb -i $f -o ${f}.out 2>client_log) >> ${f}.res 2>&1 
done
