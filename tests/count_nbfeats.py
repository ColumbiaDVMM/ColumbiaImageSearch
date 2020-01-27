from __future__ import print_function
from cufacesearch.indexer.hbase_indexer_minimal import HBaseIndexerMinimal
from cufacesearch.detector.utils import show_bbox_from_URL
import numpy as np
import base64
import sys

start_row = '0' * 40

hbim = HBaseIndexerMinimal('../conf/generated/conf_extraction_lfw_local_dlib.json')
#hbim.get_updates_from_date()
nb_face = 0
nb_image = 0
prev_row = '~'
curr_row = start_row
#print(curr_row)
file_names = []
sha1s = []
#print('Scanning', end='', flush=True)
sys.stdout.write('Scanning')
sys.stdout.flush()
while prev_row != curr_row+'~':
  prev_row = curr_row
  #if prev_row != start_row:
  prev_row += '~'
  #print(prev_row)
  #print('.', end='', flush=True)
  sys.stdout.write('.')
  sys.stdout.flush()
  for row in hbim.scan_from_row(hbim.table_sha1infos_name, row_start=prev_row, columns=['ext', 'info']):
    nb_image += 1
    file_names.append(row[1]["info:img_path"])
    for k in row[1]:
      if k.startswith("ext:dlib_feat_dlib_face") and not k.endswith("updateid") and not k.endswith("processed"):
        nb_face += 1
    curr_row = row[0]
#print('\n', flush=True)
sys.stdout.write('\n')
sys.stdout.flush()
ufn = list(set(file_names))
print(nb_image, ufn[0], len(file_names), len(ufn), nb_face)
  # url = data['info:s3_url']
  # for key in data:
  #   if key.startswith('face:'):
  #     face_bbox = key.split('face:dlib_feat_dlib_face_')[-1].split('_')
  #     feat_b64 = np.frombuffer(base64.b64decode(data[key]), dtype=np.float32)
  #     print feat_b64.shape, feat_b64
  #     show_bbox_from_URL(url, map(int, face_bbox), close_after=1)

