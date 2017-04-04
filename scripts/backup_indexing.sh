#!/bin/bash
backup_date=$(date +%Y-%m-%d)
index_dir=/srv/skaraman/cu_image_search_ht/update/indexing
backup_dir=/mnt/isi/backup_indexing/indexing-${backup_date}
if [ ! -d "$backup_dir" ]; then
  echo "Backing up indexing at ${backup_dir}"
  cmd="cp -r ${index_dir} ${backup_dir}"
  echo $cmd
  $($cmd)
fi
