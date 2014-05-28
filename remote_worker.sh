#! /bin/bash

RUBY='rvm all do ruby'
PHY=$1
ID=$2
shift 2

ssh $PHY bash -c "'cd ~/CHT-Ruby;  $RUBY worker_runner.rb -p $((20000+ID)) -n $PHY-$ID $@'"

