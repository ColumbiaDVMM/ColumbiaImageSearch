sentibank_file = "caffe_sentibank_train_iter_250000"
sentibank_prototxt = "sentibank.prototxt"
sentibank_file_sha1 = "TOBECOMPUTED"
sentibank_url = "https://www.dropbox.com/s/lv3p67m21kr3mrg/caffe_sentibank_train_iter_250000?dl=1"
imagenet_mean_file = "imagenet_mean.binaryproto"

import os
import sys
import numpy as np
from scipy import misc

from .generic_featurizer import GenericFeaturizer
from ..common.dl import download_file, mkpath


def read_binary_file(X_fn, str_precomp, list_feats_id, read_dim, read_type):
  import numpy as np
  X = []
  ok_ids = []
  with open(X_fn, "rb") as f_preout:
    for i in range(len(list_feats_id)):
      try:
        X.append(np.frombuffer(f_preout.read(read_dim), dtype=read_type))
        ok_ids.append(i)
      except Exception as inst:
        print "[read_binary_file: error] Could not read requested {} with id {}. {}".format(str_precomp,
                                                                                            list_feats_id[i], inst)
  return X, ok_ids


class SentiBankCmdLineImgFeaturizer(GenericFeaturizer):

  def __init__(self, global_conf_in, prefix="SBCMDLINEIMGFEAT_"):

    super(SentiBankCmdLineImgFeaturizer, self).__init__(global_conf_in, prefix)
    if self.verbose > 0:
      print "[{}.log] global_conf: {}".format(self.pp, self.global_conf)

    # could be loaded from conf
    self.output_blobs = ['data','fc7']
    self.device = 'CPU'
    self.features_dim = 4096
    self.read_dim = self.features_dim * 4
    self.data_read_dim = 618348
    self.read_type = np.float32
    self.dir_path = os.path.dirname(os.path.realpath(__file__))

    # Get sentibank caffe model
    self.sbcaffe_path = self.get_required_param('sbcaffe_path')
    self.caffe_exec = self.get_required_param('caffe_exec_path')
    # Test if file exits there
    if not os.path.exists(self.sbcaffe_path):
      # Download file if not
      download_file(sentibank_url, self.sbcaffe_path)

    self.imgnetmean_path = os.path.join(self.dir_path, 'data', imagenet_mean_file)
    self.init_sentibank_prototxt = os.path.join(self.dir_path, 'data', sentibank_prototxt)
    # Test if file exits there
    if not os.path.exists(self.imgnetmean_path):
      # Download file if not
      raise ValueError("Could not find mean image file at {}".format(self.imgnetmean_path))

    # We should check sha1 checksum

    # Initialize prototxt and folder
    self.init_files()
    self.cleanup = False
    # Also need to deal with imagenet mean and prototxt file...

  def __del__(self):
    from shutil import rmtree
    if self.cleanup:
      try:
        rmtree(self.tmp_dir)
      except Exception as inst:
        pass

  def set_pp(self):
    self.pp = "SentiBankCmdLineImgFeaturizer"

  def init_files(self):
    import tempfile
    self.tmp_dir = tempfile.mkdtemp()
    mkpath(os.path.join(self.tmp_dir, 'imgs/'))
    self.img_list_filename = os.path.join(self.tmp_dir, 'img_to_process.txt')
    self.features_filename = os.path.join(self.tmp_dir, 'features')
    # should copy self.init_sentibank_prototxt to self.tmp_dir
    self.sentibank_prototxt = os.path.join(self.tmp_dir, sentibank_prototxt)
    from shutil import copyfile
    copyfile(self.init_sentibank_prototxt, self.sentibank_prototxt)
    f = open(self.sentibank_prototxt)
    proto = f.read()
    f.close()
    proto = proto.replace('test.txt', self.img_list_filename)  # .replace('batch_size: 1', 'batch_size: ' + str(batch_size))
    proto = proto.replace('imagenet_mean.binaryproto', self.imgnetmean_path)
    f = open(self.sentibank_prototxt, 'w');
    f.write(proto)
    f.close()
    print "[{}:info] Initialized model.".format(self.pp)
    sys.stdout.flush()



  def featurize(self, img, bbox=None, img_type="buffer", sha1=None):
    """ Compute face feature of the face bounding box in 'd' in the image 'img'.

    :param img: image (an image buffer to be read)
    :param bbox: bounding box dictionary
    :return: sentibank image feature
    """
    # We should probably batch this process...
    import subprocess as sub

    if sha1 == None:
      # Does not work?
      # Compute sha1 if not provided...
      from ..imgio.imgio import get_SHA1_from_data
      sha1 = get_SHA1_from_data(img)
      # Seek back to properly write image to disk
      img.seek(0)

    print sha1

    img_files = [os.path.join(self.tmp_dir, 'imgs', sha1)]
    with open(img_files[0], 'wb') as fimg:
      fimg.write(img.read())

    # Create file listing images to be processed
    with open(self.img_list_filename, 'w') as f:
      f.writelines([filename + ' 0\n' for filename in img_files])

    command = self.caffe_exec + ' '  + self.sbcaffe_path + ' ' + self.sentibank_prototxt + ' ' \
              + ','.join(self.output_blobs) + ' ' \
              + ','.join([self.features_filename+'-'+feat for feat in self.output_blobs]) + ' ' \
              + str(1) + ' ' + self.device
    print "[SentiBankCmdLine.compute_features: log] command {}.".format(command)
    sys.stdout.flush()
    output, error = sub.Popen(command.split(' '), stdout=sub.PIPE, stderr=sub.PIPE).communicate()
    print "[SentiBankCmdLine.compute_features: log] output {}.".format(output)
    print "[SentiBankCmdLine.compute_features: log] error {}.".format(error)
    sys.stdout.flush()
    # os.system(command)
    os.remove(img_files[0])
    feats, ok_ids = read_binary_file(self.features_filename+'-fc7.dat', 'sbfeat', [sha1], self.read_dim, self.read_type)
    data, ok_ids = read_binary_file(self.features_filename + '-data.dat', 'data', [sha1], self.data_read_dim, self.read_type)
    return feats[0], data[0]



