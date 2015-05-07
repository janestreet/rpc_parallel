#!/bin/bash

run_test () {
    ./test_zombies.exe $@ &
    WORKER_PID=$!
    sleep 1
    ZOMBIE_COUNT=`ps a -o s,cmd | grep "[t]est_zombies" | egrep -c '^Z'`
    if [ "$ZOMBIE_COUNT" -ne "0" ]; then
        echo "FAILURE: test_zombies.exe created $ZOMBIE_COUNT zombie processes"
        exit 1
    else
        echo "SUCCESS: test_zombies.exe created no zombie processes"
        sleep 10
        PROCESS_COUNT=`ps a -o cmd | grep -c "[t]est_zombies\\.exe"`
        if [ "$PROCESS_COUNT" -ne "0" ]; then
            echo "FAILURE: test_zombies.exe didn't quit properly"
            exit 2
        else
            echo "SUCCESS: test_zombies.exe quit properly"
        fi
    fi
}

ON_HOSTNAME="-on "`hostname`

run_test ""
run_test $ON_HOSTNAME
exit 0
