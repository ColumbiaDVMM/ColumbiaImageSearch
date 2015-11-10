#!/bin/bash
nb_workers=30
sleep_time=30

for worker in $(seq 1 $nb_workers);
do
	#echo "Testing worker "$worker
	#echo "pgrep -l -f ""python fixPairwiseWorker.py "$worker" "
	worker_alive=$(pgrep -l -f "python fixPairwiseWorker.py "$worker" ")
	if [[ ! $worker_alive ]];
	then 
		echo "We should launch worker "$worker
		python fixPairwiseWorker.py $worker > logFixPairwiseWorker${worker}.txt &
	fi
	sleep $sleep_time
done
