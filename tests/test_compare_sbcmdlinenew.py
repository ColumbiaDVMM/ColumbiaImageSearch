import time
import numpy as np
from scipy import misc
from argparse import ArgumentParser
from cufacesearch.imgio.imgio import get_buffer_from_URL, get_SHA1_img_info_from_buffer
from cufacesearch.featurizer.sbcmdline_img_featurizer import SentiBankCmdLineImgFeaturizer
from cufacesearch.featurizer.sbpycaffe_img_featurizer import SentiBankPyCaffeImgFeaturizer
from cufacesearch.featurizer.featsio import featB64decode
from cufacesearch.indexer.hbase_indexer_minimal import HBaseIndexerMinimal
from cufacesearch.featurizer.generic_featurizer import test_list_sha1

#img_list = ['../cufacesearch/cufacesearch/featurizer/data/black.jpg']
img_list = []
#test_list_sha1 = ''

if __name__ == "__main__":

  parser = ArgumentParser()
  parser.add_argument("-s", "--sha1s", dest="sha1s", default=test_list_sha1)
  opts = parser.parse_args()
  print opts
  list_sha1s = opts.sha1s.split(",")
  list_sha1s= [list_sha1s[x] for x in [0, 1, 5]]
  print list_sha1s

  conf = {
    "SBCMDLINEIMGFEAT_sbcaffe_path": "./data/caffe_sentibank_train_iter_250000",
    "SBCMDLINEIMGFEAT_caffe_exec_path": "/home/ubuntu/caffe_cpu/build/tools/extract_nfeatures",
    "HBI_host": "10.1.94.57",
    "HBI_table_sha1infos": "escorts_images_sha1_infos_from_ts"
  }

  pyconf = {
    "SBPYCAFFEIMGFEAT_sbcaffe_path": "./data/caffe_sentibank_train_iter_250000",
    "SBPYCAFFEIMGFEAT_imgmean_path": "./data/imagenet_mean.npy",
  }

  diffs = []

  rows = []
  if list_sha1s[0]:
    hbi = HBaseIndexerMinimal(conf, prefix="HBI_")
    rows = hbi.get_columns_from_sha1_rows(list_sha1s, columns=["info:featnorm_cu","info:s3_url"])
  sbclif = SentiBankCmdLineImgFeaturizer(conf)
  sbpcif = SentiBankPyCaffeImgFeaturizer(pyconf)

  for row in rows:
    feat_hbase_b64 = featB64decode(row[1]["info:featnorm_cu"])
    #print feat_hbase_b64.shape
    img_url = row[1]["info:s3_url"]
    start_extr = time.time()
    img_buffer = get_buffer_from_URL(img_url)
    feat, data = sbclif.featurize(img_buffer, sha1=row[0])
    img_buffer.seek(0)
    pydata = sbpcif.preprocess_img(img_buffer)
    fpydata = pydata.flatten()
    idata = data.reshape((3, 227, 227))
    print img_url
    print idata.shape
    print pydata.shape
    misc.imsave(row[0]+"_cmd.jpg", np.swapaxes(idata, 0, 2))
    misc.imsave(row[0]+"_py.jpg", np.swapaxes(pydata, 0, 2))
    sqdiff = [np.sqrt((idata[c, :, :] - pydata[c, :, :]) ** 2) for c in range(3)]
    print [(np.sum(sqdiff[0]), np.mean(sqdiff[c]), np.max(sqdiff[c]), np.min(sqdiff[c])) for c in range(3)]
    #print idata[0, 0, :10]
    #print pydata[0, 0, :10]
    #print idata[0, 0, :10]
    #print pydata[0, 0, :10]

    #print data.shape
    #print fpydata.shape
    #print data[:10]
    #print fpydata[:10]
    img_buffer.seek(0)
    pyfeat = sbpcif.featurize(img_buffer)

    norm_pyfeat = np.linalg.norm(pyfeat)
    normed_pyfeat = pyfeat / norm_pyfeat

    norm_feat = np.linalg.norm(feat)
    normed_feat = feat/norm_feat
    proc_time = time.time() - start_extr
    diffs.append(np.linalg.norm(normed_pyfeat - normed_feat))
    print "diff: {} {} {} (processed in {}s.)".format(np.linalg.norm(feat_hbase_b64 - normed_feat),
                                                      np.linalg.norm(feat_hbase_b64 - normed_pyfeat),
                                                      np.linalg.norm(normed_pyfeat - normed_feat),
                                                      proc_time)
    # print np.linalg.norm(feat_hbase_b64)
    # print np.linalg.norm(normed_feat)

  for img in img_list:
    img_buffer = open(img, 'rb')
    sha1, format, width, height = get_SHA1_img_info_from_buffer(img_buffer)
    img_buffer.seek(0)
    start_extr = time.time()
    feat, data = sbclif.featurize(img_buffer, sha1=sha1)
    proc_time = time.time() - start_extr
    img_buffer.seek(0)
    pydata = sbpcif.transformer_preprocess(img_buffer)
    fpydata = pydata.flatten()
    idata = data.reshape((3, 227, 227))
    print idata.shape
    print pydata.shape
    misc.imsave(sha1+"_cmd.jpg", np.swapaxes(idata, 0, 2))
    misc.imsave(sha1+"_py.jpg", np.swapaxes(pydata, 0, 2))
    sqdiff = [np.sqrt((idata[c, :, :] - pydata[c, :, :]) ** 2) for c in range(3)]
    print [(np.sum(sqdiff[0]), np.mean(sqdiff[c]), np.max(sqdiff[c]), np.min(sqdiff[c])) for c in range(3)]
    #print idata[0, 0, :10]
    #print pydata[0, 0, :10]
    #print idata[0, 0, :10]
    #print pydata[0, 0, :10]

    #print data.shape
    #print fpydata.shape
    #print data[:10]
    #print fpydata[:10]
    img_buffer.seek(0)
    pyfeat = sbpcif.featurize(img_buffer)
    img_buffer.close()
    norm_pyfeat = np.linalg.norm(pyfeat)
    normed_pyfeat = pyfeat / norm_pyfeat

    norm_feat = np.linalg.norm(feat)
    normed_feat = feat/norm_feat
    diffs.append(np.linalg.norm(normed_pyfeat - normed_feat))
    print "diff: {} (processed in {}s.)".format(np.linalg.norm(normed_pyfeat - normed_feat), proc_time)

  print diffs, np.mean(diffs)