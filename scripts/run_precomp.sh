#!/bin/bash
start_date=$(date +%Y-%m-%d)
#conf_file="../conf/global_var_sample_precomp.json"
conf_file="../conf/global_var_sample_precomp_release.json"
python precompute_similar_images_parallel.py ${conf_file}  &> log_precomp_${start_date}.txt
