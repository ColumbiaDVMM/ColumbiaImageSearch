#Deprecated

from __future__ import print_function

import os
import sys
import threading
import subprocess as sub
import numpy as np


from .generic_featurizer import GenericFeaturizer
from ..common.dl import download_file, mkpath

SENTIBANK_FILE = "caffe_sentibank_train_iter_250000"
SENTIBANK_PROTOTXT = "sentibank.prototxt"
SENTIBANK_FILE_SHA1 = "TOBECOMPUTED"
SENTIBANK_URL = "https://www.dropbox.com/s/lv3p67m21kr3mrg/caffe_sentibank_train_iter_250000?dl=1"
IMAGENET_MEAN_FILE = "imagenet_mean.binaryproto"
TIMEOUT_COMMAND = 60


def read_binary_file(data_fn, str_precomp, list_feats_id, read_dim, read_type):
  data = []
  ok_ids = []
  with open(data_fn, "rb") as f_preout:
    for i in range(len(list_feats_id)):
      try:
        data.append(np.frombuffer(f_preout.read(read_dim), dtype=read_type))
        ok_ids.append(i)
      except Exception as inst:
        err_msg = "[read_binary_file: error] Could not read requested {} with id {}. {}"
        print(err_msg.format(str_precomp, list_feats_id[i], inst))
  return data, ok_ids


class TimeoutCommand(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None
        self.output = None
        self.error = None
        self.return_code = None

    def run(self, timeout):
        def target():
            self.process = sub.Popen(self.cmd.split(' '), stdout=sub.PIPE, stderr=sub.PIPE)
            self.output, self.error = self.process.communicate()

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        print('Joined thread.')
        sys.stdout.flush()
        if thread.is_alive():
            #print('Terminating process')
            #self.process.terminate()
            print('Process timed out. Killing it')
            sys.stdout.flush()
            self.process.kill()
            thread.join()

        self.return_code = self.process.returncode

    def get_return_code(self):
        return self.return_code

    def get_output(self):
        return self.output

    def get_error(self):
        return self.error


class SentiBankCmdLineImgFeaturizer(GenericFeaturizer):

  def __init__(self, global_conf_in, prefix="SBCMDLINEIMGFEAT_"):

    super(SentiBankCmdLineImgFeaturizer, self).__init__(global_conf_in, prefix)
    if self.verbose > 0:
      print("[{}.log] global_conf: {}".format(self.pp, self.global_conf))

    # could be loaded from conf
    self.output_blobs = ['data', 'fc7']
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
      download_file(SENTIBANK_URL, self.sbcaffe_path)

    self.imgnetmean_path = os.path.join(self.dir_path, 'data', IMAGENET_MEAN_FILE)
    self.init_sentibank_prototxt = os.path.join(self.dir_path, 'data', SENTIBANK_PROTOTXT)
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
      except Exception:
        pass

  def set_pp(self, pp=None):
    #self.pp = "SentiBankCmdLineImgFeaturizer"
    self.pp = "SentiBankCmdLine"

  def init_files(self):
    import tempfile
    self.tmp_dir = tempfile.mkdtemp()
    mkpath(os.path.join(self.tmp_dir, 'imgs/'))
    self.img_list_filename = os.path.join(self.tmp_dir, 'img_to_process.txt')
    self.features_filename = os.path.join(self.tmp_dir, 'features')
    # should copy self.init_sentibank_prototxt to self.tmp_dir
    self.sentibank_prototxt = os.path.join(self.tmp_dir, SENTIBANK_PROTOTXT)
    from shutil import copyfile
    copyfile(self.init_sentibank_prototxt, self.sentibank_prototxt)
    f_proto = open(self.sentibank_prototxt)
    proto = f_proto.read()
    f_proto.close()
    proto = proto.replace('test.txt', self.img_list_filename)
    # .replace('batch_size: 1', 'batch_size: ' + str(batch_size))
    proto = proto.replace('imagenet_mean.binaryproto', self.imgnetmean_path)
    f_proto = open(self.sentibank_prototxt, 'w')
    f_proto.write(proto)
    f_proto.close()
    print("[{}:info] Initialized model.".format(self.pp))
    sys.stdout.flush()


  def featurize(self, img, bbox=None, img_type="buffer", sha1=None):
    """ Compute sentibank features use command line caffe

    :param img: image (an image buffer to be read)
    :param bbox: bounding box dictionary
    :return: sentibank image feature
    """

    if sha1 is None:
      # Compute sha1 if not provided...
      from ..imgio.imgio import get_SHA1_from_buffer
      img.seek(0) # Is it needed?
      sha1 = get_SHA1_from_buffer(img)
      # Seek back to properly write image to disk
      img.seek(0)

    #if self.verbose > 1:
    print("[{}.featurize: log] Processing image: {}".format(self.pp, sha1))

    img_files = [os.path.join(self.tmp_dir, 'imgs', sha1)]
    with open(img_files[0], 'wb') as fimg:
      fimg.write(img.read())

    # Create file listing images to be processed
    with open(self.img_list_filename, 'w') as f_imglist:
      f_imglist.writelines([filename + ' 0\n' for filename in img_files])

    command = self.caffe_exec + ' '  + self.sbcaffe_path + ' ' + self.sentibank_prototxt + ' '
    command += ','.join(self.output_blobs) + ' '
    command += ','.join([self.features_filename+'-'+feat for feat in self.output_blobs]) + ' '
    command += str(1) + ' ' + self.device
    if self.verbose > 4:
      print("[{}.featurize: log] command {}.".format(self.pp, command))
      sys.stdout.flush()
    # Permission denied?
    try:
      # This can get stuck...
      #output, error = sub.Popen(command.split(' '), stdout=sub.PIPE, stderr=sub.PIPE).communicate()
      to_cmd = TimeoutCommand(command)
      to_cmd.run(TIMEOUT_COMMAND)
      if to_cmd.get_return_code() != 0:
        # https://docs.python.org/3/library/subprocess.html#subprocess.Popen.returncode
        # -6 for SIGABRT
        err_msg = "Featurization process failed with return code: {}"
        raise ValueError(err_msg.format(to_cmd.get_return_code()))
      else:
        if self.verbose > 4:
          output = to_cmd.get_output()
          error = to_cmd.get_error()
          print("[{}.featurize: log] output {}.".format(self.pp, output))
          print("[{}.featurize: log] error {}.".format(self.pp, error))
          sys.stdout.flush()
    except Exception as inst:
      # err_msg = "[{}.featurize: error] {}.".format(self.pp, inst)
      # from ..common.error import full_trace_error
      # full_trace_error(err_msg)
      raise inst


    feats, _ = read_binary_file(self.features_filename+'-fc7.dat', 'sbfeat', [sha1], self.read_dim,
                                self.read_type)
    # What is data?
    #data, ok_ids = read_binary_file(self.features_filename + '-data.dat', 'data', [sha1],
    #                                self.data_read_dim, self.read_type)
    # GenericSearcher expects self.featurizer.featurize to just return one feature...
    #return feats[0], data[0]
    return feats[0]
