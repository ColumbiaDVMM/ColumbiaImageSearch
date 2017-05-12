#!/bin/bash
start_date=$(date +%Y-%m-%d)
#conf_file="../conf/global_var_sample_precomp.json"
conf_file="../conf/global_var_sample_precomp_release.json"
python precomp_test_new_finalizer.py ${conf_file}  &> log_precomp_finalizer_${start_date}.txt
