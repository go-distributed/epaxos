#!/bin/bash

# clear logs
rm /dev/shm/demo* -fr
rm /tmp/*test* -fr

ROOT=$GOPATH/src/github.com/sargun/epaxos
cd $ROOT

echo "======= Basic Message/Data Test ======"
echo
cd message
go test -v -cover
cd ..

echo
echo "======= Static Tests ======="
echo "Epaxos State Machine Basic Test"
echo "Execution Module Test"
echo "Timeout Module Test"
echo "Persistent Module Test"
echo
cd replica
go test -v -cover
cd ..

echo
echo "======= Log Consistency Test ======"
echo
cd livetest
go test -v
cd ..

echo
echo "======= Failover Test ======"
echo
cd demo

rm log*
go build

echo "Fire 3 replicas:"
./demo -id=0 >/dev/null -v=1 -log_dir=/dev/shm -alsologtostderr 2>>log0 &
replica[0]=$!
echo "replica[0]=${replica[0]}, logfile=log0"
./demo -id=1 >/dev/null -v=1 -log_dir=/dev/shm -alsologtostderr 2>>log1 &
replica[1]=$!
echo "replica[1]=${replica[1]}, logfile=log1"
./demo -id=2 >/dev/null -v=1 -log_dir=/dev/shm -alsologtostderr 2>>log2 &
replica[2]=$!
echo "replica[2]=${replica[2]}, logfile=log2"

firsthalf=10
echo "Let them execute for $firsthalf seconds..."
echo
sleep $firsthalf

# randomly choose a victim to kill
victim=$RANDOM
let "victim %= 3"

echo "Randomly choose a victim..."
echo "Kill replica[$victim]"
kill -9 ${replica[$victim]}

break=10
echo "Let survivors continue to execute for $break seconds..."
echo
successor=$victim
let "successor += 1"
let "successor %= 3"

successor_log_begin=`cat log$successor | wc -l`
#echo $successor_log_begin

sleep $break

successor_log_end=`cat log$successor | wc -l`
#echo $successor_log_end

let "diff = $successor_log_end - $successor_log_begin"
if [[ $diff > 0 ]]; then
    echo "Still making progress? Yes"
else
    echo "Still making progress? No"
fi

echo
echo "Restore replica[$victim] from failure"
./demo -id=$victim -restore > /dev/null -v=1 -log_dir=/dev/shm -alsologtostderr 2>>log$victim &
replica[$victim]=$!

secondhalf=10
echo "Continue to execute for $secondhalf seconds..."
echo
sleep $secondhalf

echo "Stop all replicas"
kill ${replica[0]}
kill ${replica[1]}
kill ${replica[2]}

echo "Test execution consistency..."
echo

grep "From:" log0 > log0From
grep "From:" log1 > log1From
grep "From:" log2 > log2From

# check execution trace
fail=0
for i in 0 1 2
do
    grep "From: $i" log0 > log0From$i
    grep "From: $i" log1 > log1From$i
    grep "From: $i" log2 > log2From$i

    # make sure all records have the same length
    line0=`cat log0From$i | wc -l`
    line1=`cat log1From$i | wc -l`
    line2=`cat log2From$i | wc -l`

    linetmp=0
    if [[ $line0 < $line1 ]]; then
        if [[ $line0 < $line2 ]]; then
            linetmp=$line0
        else
            linetmp=$line2
        fi
    else
        if [[ $line1 < $line2 ]]; then
            linetmp=$line1
        else
            linetmp=$line2
        fi
    fi

    head -n $linetmp log0From$i > temp; mv temp log0From$i
    head -n $linetmp log1From$i > temp; mv temp log1From$i
    head -n $linetmp log2From$i > temp; mv temp log2From$i

    # doing diff on records
    diff log0From$i log1From$i > /dev/null
    if [[ $? == 0 ]]; then
        echo "PASS diff log0 log1 on $i!"
        diff log1From$i log2From$i > /dev/null
        if [[ $? == 0 ]]; then
            echo "PASS diff log1 log2 on $i!"
        else
            echo "FAIL diff log1 log2 on $i!"
            fail=1
        fi
    else
        echo "FAIL diff log0 log1 on $i"
        fail=1
    fi
done

echo
if [[ $fail == 1 ]]; then
    echo "Execution result test FAILED! Please check the log!!!"
else
    echo "Execution result test PASS!"
fi
