#!/bin/bash
backup_date=$(date +%Y-%m-%d)
index_dir=/srv/skaraman/cu_image_search_ht/update/indexing
backup_dir=/mnt/isi/backup_indexing/indexing-${backup_date}
necessary_files=("update_list*.txt" "sha1_mapping" "comp_features" "comp_idx" "hash_bits")
if [ ! -d "$backup_dir" ]; then
  echo "Backing up indexing at ${backup_dir}"
  for onefile in ${necessary_files[@]}
  do
    cmd="cp -r ${index_dir}/${onefile} ${backup_dir}"
    echo $cmd
    $($cmd)
  done
fi
