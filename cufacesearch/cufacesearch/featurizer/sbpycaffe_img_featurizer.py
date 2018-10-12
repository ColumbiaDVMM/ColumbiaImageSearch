sentibank_file = "caffe_sentibank_train_iter_250000"
sentibank_prototxt = "pycaffe_sentibank.prototxt"
#sentibank_prototxt = "pycaffemem_sentibank.prototxt"
sentibank_file_sha1 = "TOBECOMPUTED"
sentibank_url = "https://www.dropbox.com/s/lv3p67m21kr3mrg/caffe_sentibank_train_iter_250000?dl=1"
imagenet_mean_npy_urldlpath = "https://www.dropbox.com/s/s5oqp801tgiktra/imagenet_mean.npy?dl=1"

import os
import sys
try:
  import caffe
except:
  print("Could not import caffe. You will not be able to use 'SentiBankPyCaffeImgFeaturizer'")
import numpy as np
from scipy import misc

from .generic_featurizer import GenericFeaturizer
from ..common.dl import download_file

# Is there a memory leak somewhere in this featurizer? In caffe?

class SentiBankPyCaffeImgFeaturizer(GenericFeaturizer):
  """Sentibank image featurizer using pycaffe.
  """

  def __init__(self, global_conf_in, prefix="SBPYCAFFEIMGFEAT_"):
    """Sentibank image featurizer constructor.

    :param global_conf_in: configuration file or dictionary
    :type global_conf_in: str, dict
    :param prefix: prefix in configuration
    :type prefix: str
    """

    super(SentiBankPyCaffeImgFeaturizer, self).__init__(global_conf_in, prefix)
    self.set_pp(pp="SentiBankPyCaffeImgFeaturizer")
    if self.verbose > 0:
      print("[{}.log] global_conf: {}".format(self.pp, self.global_conf))

    # could be loaded from conf
    self.output_blobs = ['fc7']
    self.target_size = (256, 256, 3)
    self.crop_size = (227, 227)
    self.reorder_dim = [2, 1, 0]
    #interp : str, optional
    #Interpolation to use for re-sizing ('nearest', 'lanczos', 'bilinear', 'bicubic' or 'cubic').
    self.resize_type = 'lanczos' #[lowest error 0.169]
    #self.resize_type = 'bilinear' #[error 0.22]
    #self.resize_type = 'bicubic' #[error 0.181]
    #self.resize_type = 'cubic' #[error 0.181]

    self.net = None
    #self.transformer = None
    self.dir_path = os.path.dirname(os.path.realpath(__file__))

    # Get sentibank caffe model
    self.sbcaffe_path = str(self.get_required_param('sbcaffe_path'))
    # Test if file exits there
    if not os.path.exists(self.sbcaffe_path):
      # Download file if not
      download_file(sentibank_url, self.sbcaffe_path)

    self.imgnetmean_path = str(self.get_required_param('imgmean_path'))
    # Test if file exits there
    if not os.path.exists(self.imgnetmean_path):
      # Download file if not
      download_file(imagenet_mean_npy_urldlpath, self.imgnetmean_path)

    self.imgmean = np.load(self.imgnetmean_path)
    self.imgmean = np.swapaxes(self.imgmean, 0, 2)

    # Set crop boundaries
    self.w_boff = int((self.imgmean.shape[0] - self.crop_size[0]) / 2.0)
    self.h_boff = int((self.imgmean.shape[1] - self.crop_size[1]) / 2.0)
    self.w_eoff = self.w_boff + self.crop_size[0]
    self.h_eoff = self.h_boff + self.crop_size[1]

    # Crop image mean
    self.mu = self.imgmean[self.w_boff:self.w_eoff, self.h_boff:self.h_eoff, :]
    self.mu = np.swapaxes(self.mu, 0, 2)

    #print self.w_off, self.h_off

    # We should check sha1 checksum

    # Initialize model and transformer
    self.init_model()
    # produces very different results
    self.init_transformer()

  def init_model(self):
    """Initialize sentibank model in caffe."""
    # We could be exposed the mode as a parameter if we ever want to run extractions on GPU
    caffe.set_mode_cpu()

    model_def = os.path.join(self.dir_path, 'data', sentibank_prototxt)
    model_weights = self.sbcaffe_path

    self.net = caffe.Net(model_def, model_weights, caffe.TEST)
    print("[{}:info] Initialized model.".format(self.pp))
    sys.stdout.flush()

  def init_transformer(self):
    """Initialize transformer that pre-process an image to fit caffe format."""
    # Create transformer for the input called 'data'
    self.transformer = caffe.io.Transformer({'data': self.net.blobs['data'].data.shape})
    self.transformer.set_transpose('data', (2, 0, 1))  # move image channels to outermost dimension
    self.transformer.set_mean('data', self.mu)  # subtract the dataset-mean image
    self.transformer.set_channel_swap('data', (2, 1, 0))
    print("[{}:info] Initialized transformer.".format(self.pp))
    sys.stdout.flush()

  def preprocess_img(self, img_buffer):
    """Preprocess image to fit caffe model format.

    :param img_buffer: input image
    :type img_buffer: :class:`numpy.ndarray`
    :return: preprocessed image
    :rtype: :class:`numpy.ndarray`
    """
    # This can fail with a memory error for big images...
    image = caffe.io.load_image(img_buffer)
    # Fix for GIF
    if len(image.shape)==4:
      # Get first 'frame' of GIF
      image = np.squeeze(image[1,:,:,:])
    img_resize = misc.imresize(image, self.target_size, interp=self.resize_type)
    
    # Take a central crop of 227x227.
    # The model was trained with random crops of this dimension
    # and existing features were extracted with a central crop like this
    img_out = img_resize[self.w_boff:self.w_eoff, self.h_boff:self.h_eoff, :]

    return self.transformer.preprocess('data', img_out)


  def featurize(self, img, bbox=None, img_type="buffer"):
    """ Compute sentibank feature of image `img`. `bbox` is ignored.

    :param img: input image
    :type img: :class:`numpy.ndarray`
    :param bbox: bounding box dictionary
    :type bbox: dict
    :return: sentibank image feature
    :rtype: :class:`numpy.ndarray`
    """
    # TODO: could use bbox to pre-crop the image...
    # Could anything block here?
    preprocessed_img = self.preprocess_img(img)
    self.net.blobs['data'].data[...] = preprocessed_img

    _ = self.net.forward(blobs=self.output_blobs)
    #return [np.squeeze(self.net.blobs[x].data) for x in self.output_blobs]
    return np.squeeze(self.net.blobs['fc7'].data)




