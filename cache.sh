#!/bin/bash
killall vmtouch
#base_update_path="/home/ubuntu/memex/update/indexing/indexing-2017-04-04"
base_update_path="/home/ubuntu/memex/update/indexing/"
repo_path="/home/ubuntu/memex/ColumbiaImageSearch/"
${repo_path}/tools/vmtouch -m 400G -vld ${base_update_path}/sha1_mapping ${base_update_path}/hash_bits/ ${base_update_path}/comp_features/ ${base_update_path}/comp_idx/

#/home/ubuntu/memex/vmtouch -m 400G -vld /home/ubuntu/memex/
#/home/ubuntu/memex/vmtouch -m 400G -vld /home/ubuntu/memex/update/indexing/sha1_mapping /home/ubuntu/memex/update/indexing/hash_bits/ /home/ubuntu/memex/update/indexing/comp_features/ /home/ubuntu/memex/update/indexing/comp_idx/
