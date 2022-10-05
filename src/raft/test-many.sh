#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT

# Note: because the socketID is based on the current userID,
# ./test-mr.sh cannot be run in parallel
runs=$1
sum=0

rm output*

for i in $(seq 1 $runs); do
    if ! go test -race -run 2A > output.log 2>&1; then
        sum=$((sum + 1))
        echo '***' FAILED TESTS IN TRIAL $i
        echo '***' FAILED $sum TESTS THUS FAR
        cp output.log output-$i.log
    else 
        echo '***' PASSED TESTS IN TRIAL $i
    fi
done
echo '***' FAILED $sum TESTING TRIALS