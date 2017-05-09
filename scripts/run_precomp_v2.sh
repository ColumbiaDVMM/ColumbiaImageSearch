#!/bin/bash
start_date=$(date +%Y-%m-%d)
conf_file="../conf/global_var_sample_precomp_release.json"
version="_v2"
python precompute_similar_images_parallel${version}.py ${conf_file}  &> log_precomp${version}_${start_date}.txt
