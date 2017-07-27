from cufacesearch.indexer.hbase_indexer_minimal import HBaseIndexerMinimal
from cufacesearch.detector.utils import show_face_from_URL
import numpy as np
import base64

hbim = HBaseIndexerMinimal('../conf/global_conf_test_get_face_hbase.json')

list_sha1s = ['000000D29139258BD3716C94A68CFF54A8A7C033', '000001BF13372B9665A89ED25E8948FC7F99F7F1']
rows = hbim.get_columns_from_sha1_rows(list_sha1s, ['face', 'info:s3_url'])

for sha1, data in rows:
  print sha1, data
  url = data['info:s3_url']
  for key in data:
    if key.startswith('face:'):
      face_bbox = key.split('face:dlib_feat_dlib_face_')[-1].split('_')
      feat_b64 = np.frombuffer(base64.b64decode(data[key]), dtype=np.float32)
      print feat_b64.shape, feat_b64
      show_face_from_URL(url, face_bbox, close_after=1)
