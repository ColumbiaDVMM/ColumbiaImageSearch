import numpy as np
from argparse import ArgumentParser
from cufacesearch.imgio.imgio import get_buffer_from_URL
from cufacesearch.featurizer.sbcmdline_img_featurizer import SentiBankCmdLineImgFeaturizer
from cufacesearch.featurizer.featsio import featB64decode
from cufacesearch.indexer.hbase_indexer_minimal import HBaseIndexerMinimal
from cufacesearch.featurizer.generic_featurizer import test_list_sha1

if __name__ == "__main__":

  parser = ArgumentParser()
  parser.add_argument("-s", "--sha1s", dest="sha1s", default=test_list_sha1)
  opts = parser.parse_args()
  print opts
  list_sha1s = opts.sha1s.split(",")

  conf = {
    "SBCMDLINEIMGFEAT_sbcaffe_path": "./data/caffe_sentibank_train_iter_250000",
    "SBCMDLINEIMGFEAT_caffe_exec_path": "/home/ubuntu/caffe_cpu/build/tools/extract_nfeatures",
    "HBI_host": "10.1.94.57",
    "HBI_table_sha1infos": "escorts_images_sha1_infos_from_ts"
  }

  hbi = HBaseIndexerMinimal(conf, prefix="HBI_")
  rows = hbi.get_columns_from_sha1_rows(list_sha1s, columns=["info:featnorm_cu","info:s3_url"])
  sbclif = SentiBankCmdLineImgFeaturizer(conf)

  for row in rows:
    feat_hbase_b64 = featB64decode(row[1]["info:featnorm_cu"])
    print feat_hbase_b64.shape
    img_url = row[1]["info:s3_url"]
    img_buffer = get_buffer_from_URL(img_url)
    feat = sbclif.featurize(img_buffer, sha1=row[0])
    print feat.shape
    norm_feat = np.linalg.norm(feat)
    normed_feat = feat/norm_feat
    print np.linalg.norm(feat_hbase_b64-normed_feat)
    # print np.linalg.norm(feat_hbase_b64)
    # print np.linalg.norm(normed_feat)