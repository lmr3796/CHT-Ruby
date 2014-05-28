#! /usr/bin/env bash

SPEC_ROOT=${SPEC_ROOT:=${HOME}/SPEC_BUILD}
for (( i=0 ; $i<15 ; i=i+1 ))
do
    wc -w $SPEC_ROOT/benchspec/CPU2006/401.bzip2/data/all/input/input.combined | awk '{print $1}' #> /dev/null
done
