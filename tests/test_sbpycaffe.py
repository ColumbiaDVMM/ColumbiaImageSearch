from cufacesearch.imgio.imgio import get_buffer_from_URL
from cufacesearch.featurizer.sbpycaffe_img_featurizer import SentiBankPyCaffeImgFeaturizer

if __name__ == "__main__":

  conf = {"SBPYCAFFEIMGFEAT_sbcaffe_path": "./data/caffe_sentibank_train_iter_250000"}
  sbpcif = SentiBankPyCaffeImgFeaturizer(conf)

  img_url = "http://www.cs.cmu.edu/~chuck/lennapg/len_top.jpg"
  img_buffer = get_buffer_from_URL(img_url)
  feat = sbpcif.featurize(img_buffer)
  print feat