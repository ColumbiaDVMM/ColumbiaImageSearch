pred_bz2_file = "shape_predictor_68_face_landmarks.dat.bz2"
www_pred_path = "http://dlib.net/files/"+pred_bz2_file
rec_bz2_file = "dlib_face_recognition_resnet_model_v1.dat.bz2"
www_rec_path = "http://dlib.net/files/"+rec_bz2_file


def download_model(url, local_path, bz2_file):
  import os
  import urllib
  print "Downloading model from: {}".format(url)
  out_dir = os.path.dirname(local_path)
  try:
    os.makedirs(out_dir)
  except:
    pass
  bz2_local_path = os.path.join(out_dir,bz2_file)
  urllib.urlretrieve(url, bz2_local_path)
  # need to unzip
  unzip_model(bz2_local_path, local_path)


def unzip_model(bz2_model_path, local_path):
  print "Unzipping file: {}".format(bz2_model_path)
  import bz2
  bz2model = bz2.BZ2File(bz2_model_path, 'r')
  with open(local_path, 'w') as out:
    data = bz2model.read()
    out.write(data)


class DLibFeaturizer(object):

  def __init__(self, global_conf_filename, prefix="DLIBFEAT_"):
    #TODO read predictor_path, face_rec_model_path from conf
    import dlib
    import json
    import os
    self.global_conf_filename = global_conf_filename
    self.prefix = prefix
    self.verbose = 0
    self.global_conf = json.load(open(global_conf_filename, 'rt'))
    pred_path = str(self.get_param('pred_path'))
    if not pred_path:
      raise ValueError('[DLibFeaturizer: error] pred_path was not set in config file.')
    # test if file exits there
    if not os.path.exists(pred_path):
      # download file if not
      download_model(www_pred_path, pred_path, pred_bz2_file)
    self.sp = dlib.shape_predictor(pred_path)
    rec_path = str(self.get_param('rec_path'))
    if not pred_path:
      raise ValueError('[DLibFeaturizer: error] pred_path was not set in config file.')
    # test if file exits there
    if not os.path.exists(rec_path):
      # download file if not
      download_model(www_rec_path, rec_path, rec_bz2_file)
    self.facerec = dlib.face_recognition_model_v1(rec_path)


  def get_param(self, param):
    key_param = self.prefix + param
    if key_param in self.global_conf:
      return self.global_conf[key_param]
    if self.verbose:
      print '[get_param: info] could not find {} in configuration'.format(key_param)

  def featurize(self, img, d):
    from dlib import rectangle
    dlib_bbox = rectangle(d['left'], d['top'], d['right'], d['bottom'])
    shape = self.sp(img, dlib_bbox)
    return self.facerec.compute_face_descriptor(img, shape)