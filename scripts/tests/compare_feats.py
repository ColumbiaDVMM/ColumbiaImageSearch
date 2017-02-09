import numpy as np
import base64
import happybase

tab_name = "escorts_images_sha1_infos_from_ts"
sha1s_to_compare = ["B4A7F10988DED0CDD967BB1EAAB92E3CB16A2305", "5A18A10A0577A5242A2D69B326BFF2E0B6E912DF", "4542847FBE9F6EBB1A68EB9F9E2572D8EEB343DE"]

conn = happybase.Connection(host='10.1.94.57')
table = conn.table(tab_name)
rows = table.rows(sha1s_to_compare)
feats = []
for row in rows:
    feats.append(np.frombuffer(base64.b64decode(row[1]['info:featnorm_cu']),np.float32))
    
for i,feat_1 in enumerate(feats):
    for j,feat_2 in enumerate(feats[i+1:]):
        print("Dist {}-{} is: {}".format(sha1s_to_compare[i], sha1s_to_compare[j+i+1], np.linalg.norm(feat_1-feat_2)))
