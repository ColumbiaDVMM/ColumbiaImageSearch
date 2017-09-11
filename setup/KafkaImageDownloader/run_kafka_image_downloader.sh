#!/bin/bash

python ../../scripts/ingestion/run_image_ingestion.py -t -d -c ../../conf/global_conf_facesearch_kafka_hg_release.json &> log_image_ingestion_$(date +%Y-%m-%d).txt


