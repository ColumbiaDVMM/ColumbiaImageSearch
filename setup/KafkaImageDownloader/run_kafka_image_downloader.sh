#!/bin/bash
# TODO: set this, test or release?
suffix="_test"
#suffix="_release"

python ../../scripts/ingestion/run_image_ingestion.py -t -d -c ../../conf/conf_kafka_image_downloader${suffix}.json &> log_image_ingestion${suffix}_$(date +%Y-%m-%d).txt


