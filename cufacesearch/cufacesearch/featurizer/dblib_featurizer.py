pred_bz2_file = "shape_predictor_68_face_landmarks.dat.bz2"
www_pred_path = "http://dlib.net/files/"+pred_bz2_file
rec_bz2_file = "dlib_face_recognition_resnet_model_v1.dat.bz2"
www_rec_path = "http://dlib.net/files/"+rec_bz2_file

from .generic_featurizer import GenericFeaturizer
import os
import dlib

def download_model(url, local_path, bz2_file):
  """ Download model from 'url' to the directory of 'local_path' and unzip to 'local_path'.

  :param url: url of model to download
  :param local_path: final local path of unzipped model
  :param bz2_file: bz2 local filename
  """
  import urllib
  print "Downloading model from: {}".format(url)
  out_dir = os.path.dirname(local_path)
  try:
    os.makedirs(out_dir)
  except:
    pass
  bz2_local_path = os.path.join(out_dir, bz2_file)
  urllib.urlretrieve(url, bz2_local_path)
  # Need to unzip
  unzip_model(bz2_local_path, local_path)


def unzip_model(bz2_model_path, local_path):
  """ Unzip dlib model from 'bz2_model_path' to 'local_path'.

  :param bz2_model_path: input file to unzip.
  :param local_path: output path.
  """
  print "Unzipping file: {}".format(bz2_model_path)
  import bz2
  bz2model = bz2.BZ2File(bz2_model_path, 'r')
  with open(local_path, 'w') as out:
    data = bz2model.read()
    out.write(data)


class DLibFeaturizer(GenericFeaturizer):

  def __init__(self, global_conf_in, prefix="DLIBFEAT_"):
    super(DLibFeaturizer, self).__init__(global_conf_in, prefix)
    if self.verbose > 0:
      print "[DLibFeaturizer.log] global_conf: {}".format(self.global_conf)

    # Get shape predictor
    pred_path = self.get_required_param('pred_path')
    if not pred_path:
      raise ValueError('[DLibFeaturizer: error] pred_path was not set in config file.')
    # Test if file exits there
    if not os.path.exists(pred_path):
      # Download file if not
      download_model(www_pred_path, pred_path, pred_bz2_file)
    # Intialize shape predictor
    self.sp = dlib.shape_predictor(str(pred_path))

    # Get recognizer model
    rec_path = self.get_required_param('rec_path')
    if not pred_path:
      raise ValueError('[DLibFeaturizer: error] pred_path was not set in config file.')
    # Test if file exits there
    if not os.path.exists(rec_path):
      # Download file if not
      download_model(www_rec_path, rec_path, rec_bz2_file)
    # Initialize recognizer model
    self.facerec = dlib.face_recognition_model_v1(str(rec_path))

  def set_pp(self):
    self.pp = "DLibFeaturizer"

  def featurize(self, img, d):
    """ Compute face feature of the face bounding box in 'd' in the image 'img'.

    :param img: image (a scikit-image)
    :param d: bounding box dictionary
    :return: face feature
    """
    # Deal with B&W images
    if len(img.shape)==2:
      import skimage
      img = skimage.color.gray2rgb(img)
    # Build dlib rectangle from bounding box
    from dlib import rectangle
    dlib_bbox = rectangle(d['left'], d['top'], d['right'], d['bottom'])
    shape = self.sp(img, dlib_bbox)
    # Return feature
    # should we force features to be np.float32 or np.float64?
    return self.facerec.compute_face_descriptor(img, shape)