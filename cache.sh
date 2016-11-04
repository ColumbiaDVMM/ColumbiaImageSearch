#!/usr/bin/bash
killall vmtouch
#/home/ubuntu/memex/vmtouch -m 400G -vld /home/ubuntu/memex/
/home/ubuntu/memex/vmtouch -m 400G -vld /home/ubuntu/memex/update/indexing/sha1_mapping /home/ubuntu/memex/update/indexing/hash_bits/ /home/ubuntu/memex/update/indexing/comp_features/ /home/ubuntu/memex/update/indexing/comp_idx/
