tfsentibank_file_sha1 = "TOBECOMPUTED"
imagenet_mean_npy_urldlpath = "https://www.dropbox.com/s/s5oqp801tgiktra/imagenet_mean.npy?dl=1"
tfsentibank_npy_urldlpath = "https://www.dropbox.com/s/3d938qmtm6kngoo/tfdeepsentibank.npy?dl=1"
padding_type = 'VALID'

import os
import sys
import time
import numpy as np

from kaffetensorflow.network import Network
import tensorflow as tf
from scipy import misc

from .generic_featurizer import GenericFeaturizer



class DeepSentibankNet(Network):

  def setup(self):
    (self.feed('data')
     .conv(11, 11, 96, 4, 4, padding=padding_type, name='conv1')
     .max_pool(3, 3, 2, 2, padding=padding_type, name='pool1')
     .lrn(2, 2e-05, 0.75, name='norm1')
     .conv(5, 5, 256, 1, 1, group=2, name='conv2')
     .max_pool(3, 3, 2, 2, padding=padding_type, name='pool2')
     .lrn(2, 2e-05, 0.75, name='norm2')
     .conv(3, 3, 384, 1, 1, name='conv3')
     .conv(3, 3, 384, 1, 1, group=2, name='conv4')
     .conv(3, 3, 256, 1, 1, group=2, name='conv5')
     .max_pool(3, 3, 2, 2, padding=padding_type, name='pool5')
     .fc(4096, name='fc6')
     .fc(4096, name='fc7')
     .fc(2089, relu=False, name='fc8-t')
     .softmax(name='prob'))


class DeepSentibankExtractor(object):
  def __init__(self, modelpath, imgmeanpath):
    tf.reset_default_graph()
    self.session = tf.Session()
    self.modelpath = modelpath
    # these settings are dependent of the deepsentibank model
    self.target_size = (256, 256, 3)
    self.crop_size = (227, 227)
    self.reorder_dim = [2, 1, 0]
    self.net_input_size = (1, self.crop_size[0], self.crop_size[1], 3)
    # this seems to give the most similar features as those computed from caffe ...
    # ... but caffe seems to use cv2 resize with default bilinear interpolation
    self.resize_type = 'bicubic'
    # self.resize_type = 'bilinear'
    # init mean image
    self.imgmean = np.load(imgmeanpath)
    self.imgmean = np.swapaxes(self.imgmean, 0, 2)
    self.net = None
    self.init_net()

  # setup network
  def init_net(self):
    print "[init_net] Loading DeepSentibank net from {}...".format(self.modelpath)
    sys.stdout.flush()
    start_init = time.time()
    self.input_data = tf.placeholder(tf.float32, shape=self.net_input_size)
    self.session.run(tf.global_variables_initializer())
    self.net = DeepSentibankNet({'data': self.input_data}, trainable=False)
    # Load the data
    self.net.load(self.modelpath, self.session)
    print "[init_net] Loaded DeepSentibank net in {}s".format(time.time() - start_init)
    sys.stdout.flush()

  def preprocess_img(self, img_out):
    # Deal with BW images
    if len(img_out.shape) == 2:
      img_out = np.dstack([img_out] * 3)
    # Resize to target size (256, 256)
    img_resize = misc.imresize(img_out, size=self.target_size, interp=self.resize_type)
    # We need to reorder RGB -> BGR as model was initially trained with Caffe
    img_reorder = img_resize[:, :, self.reorder_dim]
    # We need to subtract imagenet image mean
    img_meansub = (img_reorder - self.imgmean)
    # Take a central crop of 227x227.
    # The model was trained with random crops of this dimension
    # and existing features were extracted with a central crop like this
    w_off = int((img_meansub.shape[0] - self.crop_size[0]) / 2.0)
    h_off = int((img_meansub.shape[1] - self.crop_size[1]) / 2.0)
    img_out = img_meansub[w_off:w_off + self.crop_size[0], h_off:h_off + self.crop_size[1], :]
    return img_out


  # extract from image
  def get_features_from_img_buffer(self, img_buffer, features=['fc7'], verbose=False):
    img_out = misc.imread(img_buffer)
    img = self.preprocess_img(img_out)
    output = self.session.run(self.net.get_output(out_layers=features),
                              feed_dict={self.input_data: np.asarray([img.astype("float32")])})
    return output

class SentiBankTensorflowImgFeaturizer(GenericFeaturizer):

  def __init__(self, global_conf_in, prefix="SBTFIMGFEAT_"):
    super(SentiBankTensorflowImgFeaturizer, self).__init__(global_conf_in, prefix)
    if self.verbose > 0:
      print "[{}.log] global_conf: {}".format(self.pp, self.global_conf)

    # could be loaded from conf
    self.output_blobs = ['fc7']

    self.net = None
    self.transformer = None
    self.dir_path = os.path.dirname(os.path.realpath(__file__))

    print self.global_conf
    sys.stdout.flush()

    # Get sentibank tf model and imagenet mean
    self.model_path = self.get_required_param('model_path')
    # Test if file exits there
    if not os.path.exists(self.model_path):
      # Download file if not
      download_file(tfsentibank_npy_urldlpath, self.model_path)
    self.imgnetmean_path = self.get_required_param('imgmean_path')
    # Test if file exits there
    if not os.path.exists(self.imgnetmean_path):
      # Download file if not
      download_file(imagenet_mean_npy_urldlpath, self.imgnetmean_path)

    # We should check sha1 checksum

    # Initialize model
    self.init_model()

  def set_pp(self):
    self.pp = "SentiBankTensorflowImgFeaturizer"

  def init_model(self):
    self.DSE = DeepSentibankExtractor(self.model_path, self.imgnetmean_path)
    print "[{}:info] Initialized model.".format(self.pp)
    sys.stdout.flush()

  def featurize(self, img_buffer, bbox=None, img_type="buffer"):
    """ Compute face feature of the face bounding box in 'd' in the image 'img'.

    :param img: image (an image buffer to be read)
    :param bbox: bounding box dictionary
    :return: sentibank image feature
    """
    return self.DSE.get_features_from_img_buffer(img_buffer)[0]



