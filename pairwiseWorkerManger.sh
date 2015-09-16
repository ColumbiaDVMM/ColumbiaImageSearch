#!/bin/bash
nb_workers=4
sleep_time=5
batch_size=100

while true;
do
	for worker in $(seq 1 $nb_workers);
	do
		echo "Testing worker "$worker
		worker_alive=$(pgrep -l -f "python fillPairwise.py "$worker"")
		if [[ ! $worker_alive ]];
		then 
			echo "We should launch worker "$worker
			python fillPairwise.py $worker $batch_size&
		fi
		sleep $sleep_time
	done
done
