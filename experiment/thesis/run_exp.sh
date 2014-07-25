#! /bin/bash
set -e
echo '' > current.log
for f in $@; do
	echo "Running $f" >> current.log
	(time ./workload_executer.rb -i $f -o ${f}.out 2>client.log) >> ${f}.res 2>&1 
done
