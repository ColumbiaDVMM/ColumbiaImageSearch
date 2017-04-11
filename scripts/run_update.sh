#!/bin/bash
start_date=$(date +%Y-%m-%d)
python continuous_update_hbase.py ../conf/global_var_remotehbase_release.json  &> log_update_${start_date}.txt
