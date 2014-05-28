#! /usr/bin/env bash

DATA_SIZE=$1
ITERATION=1
SPEC_ROOT=${SPEC_ROOT:=${HOME}/SPEC_BUILD}
SUITE_PATH=$SPEC_ROOT/benchspec/CPU2006/464.h264ref/run/run_base_train_amd64-m64-gcc43-nn.0000
SUITE=464

cd $SPEC_ROOT
source shrc
cd $SUITE_PATH

runspec --noreportable --size=$DATA_SIZE --iterations=$ITERATION $SUITE
