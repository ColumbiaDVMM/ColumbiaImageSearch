sentibank_file = "caffe_sentibank_train_iter_250000"
sentibank_prototxt = "pycaffe_sentibank.prototxt"
#sentibank_prototxt = "pycaffemem_sentibank.prototxt"
sentibank_file_sha1 = "TOBECOMPUTED"
sentibank_url = "https://www.dropbox.com/s/lv3p67m21kr3mrg/caffe_sentibank_train_iter_250000?dl=1"
imagenet_mean_npy_urldlpath = "https://www.dropbox.com/s/s5oqp801tgiktra/imagenet_mean.npy?dl=1"

import os
import sys
import caffe
import numpy as np
from scipy import misc

from .generic_featurizer import GenericFeaturizer
from ..common.dl import download_file


class SentiBankPyCaffeImgFeaturizer(GenericFeaturizer):

  def __init__(self, global_conf_in, prefix="SBPYCAFFEIMGFEAT_"):
    super(SentiBankPyCaffeImgFeaturizer, self).__init__(global_conf_in, prefix)
    if self.verbose > 0:
      print "[{}.log] global_conf: {}".format(self.pp, self.global_conf)

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
    self.sbcaffe_path = self.get_required_param('sbcaffe_path')
    # Test if file exits there
    if not os.path.exists(self.sbcaffe_path):
      # Download file if not
      download_file(sentibank_url, self.sbcaffe_path)

    self.imgnetmean_path = self.get_required_param('imgmean_path')
    # Test if file exits there
    if not os.path.exists(self.imgnetmean_path):
      # Download file if not
      download_file(imagenet_mean_npy_urldlpath, self.imgnetmean_path)

    self.imgmean = np.load(self.imgnetmean_path)
    print self.imgmean.shape
    self.imgmean = np.swapaxes(self.imgmean, 0, 2)
    print self.imgmean.shape

    # We should check sha1 checksum

    # Initialize model and transformer
    self.init_model()
    # produces very different results
    self.init_transformer()

    # Also need to deal with imagenet mean and prototxt file...

  def set_pp(self):
    self.pp = "SentiBankPyCaffeImgFeaturizer"

  def init_model(self):
    caffe.set_mode_cpu()

    model_def = os.path.join(self.dir_path, 'data', sentibank_prototxt)
    model_weights = self.sbcaffe_path

    self.net = caffe.Net(model_def, model_weights, caffe.TEST)
    print "[{}:info] Initialized model.".format(self.pp)
    sys.stdout.flush()

  def init_transformer(self):
    mu = np.load(os.path.join(self.dir_path, 'data', 'imagenet_mean.npy'))
    #print mu.shape
    self.imgmean = np.swapaxes(mu, 0, 2)
    #print self.imgmean.shape
    ## Should we average?
    #mu = mu.mean(1).mean(1)  # average over pixels to obtain the mean (BGR) pixel values
    #print 'mean-subtracted values:', zip('BGR', mu)
    # Or should we crop?
    #mu = mu[:, 13:240, 13:240]
    mu = mu[:, 14:241, 14:241]
    #mu = mu[:, 15:242, 15:242]
    #print mu.shape

    # Transformer should reproduce what happens when running command line caffe with a test.txt file containing an image
    # type: IMAGE_DATA
    # image_data_param
    # {
    #   source: "test.txt"
    #   batch_size: 1
    #   new_height: 256
    #   new_width: 256
    # }
    # transform_param
    # {
    #   mirror: false
    #   crop_size: 227
    #   mean_file: "./features_extract/imagenet_mean.binaryproto"
    # }

    # create transformer for the input called 'data'
    self.transformer = caffe.io.Transformer({'data': self.net.blobs['data'].data.shape})
    # Which transformation are needed?
    self.transformer.set_transpose('data', (2, 0, 1))  # move image channels to outermost dimension
    self.transformer.set_mean('data', mu)  # subtract the dataset-mean value in each channel
    #self.transformer.set_raw_scale('data', 255)  # rescale from [0, 1] to [0, 255]
    self.transformer.set_channel_swap('data', (2, 1, 0))
    print "[{}:info] Initialized transformer.".format(self.pp)
    sys.stdout.flush()

  def transformer_preprocess(self, img_buffer):
    image = caffe.io.load_image(img_buffer)
    print "image.shape", image.shape
    img_resize = misc.imresize(image, self.target_size, interp=self.resize_type)
    
    # Take a central crop of 227x227.
    # The model was trained with random crops of this dimension
    # and existing features were extracted with a central crop like this
    w_off = int((img_resize.shape[0] - self.crop_size[0]) / 2.0)
    h_off = int((img_resize.shape[1] - self.crop_size[1]) / 2.0)
    #print "w_off, h_off:", w_off, h_off
    img_out = img_resize[w_off:w_off + self.crop_size[0], h_off:h_off + self.crop_size[1], :]
    #img_out = img_resize[14:241, 14:241, :]
    #img_out = img_resize[13:240, 13:240, :]
    return self.transformer.preprocess('data', img_out)

  def preprocess_img(self, img_buffer):
    # This works OK for tensorflow...
    img_out = misc.imread(img_buffer)
    # Deal with BW images
    if len(img_out.shape) == 2:
      img_out = np.dstack([img_out] * 3)
    # Resize to target size (256, 256)
    img_resize = misc.imresize(img_out, self.target_size, interp=self.resize_type)
    #print "img_resize", img_resize.shape
    # We need to reorder RGB -> BGR as model was initially trained with Caffe
    img_reorder = img_resize[:, :, self.reorder_dim]
    #print "img_reorder", img_reorder.shape
    # We need to subtract imagenet image mean
    img_meansub = (img_reorder - self.imgmean)
    # Take a central crop of 227x227.
    # The model was trained with random crops of this dimension
    # and existing features were extracted with a central crop like this
    w_off = int((img_meansub.shape[0] - self.crop_size[0]) / 2.0)
    h_off = int((img_meansub.shape[1] - self.crop_size[1]) / 2.0)
    img_out = img_meansub[w_off:w_off + self.crop_size[0], h_off:h_off + self.crop_size[1], :]
    # Move color axis to front
    img_out = np.rollaxis(img_out, 2, 0)
    # Add one axis i.e batch size of 1
    img_out = img_out[np.newaxis, :, :, :]
    print img_out.shape
    print img_out[0][0][0][0]
    return img_out


  def featurize(self, img, bbox=None, img_type="buffer"):
    """ Compute face feature of the face bounding box in 'd' in the image 'img'.

    :param img: image (an image buffer to be read)
    :param bbox: bounding box dictionary
    :return: sentibank image feature
    """
    # image = caffe.io.load_image(img)
    # print image.shape
    #
    # # # # Should the image be resized to 256x256 and then get a central crop? More difference
    # # image.resize((256, 256, 3))
    # # print image.shape
    # # image = image[15:242, 15:242, :]
    # # print image.shape
    # transformed_image = self.transformer.preprocess('data', image)

    #transformed_image = self.preprocess_img(img)
    #self.net.blobs['data'].data[...] = transformed_image

    transformed_image = self.transformer_preprocess(img)
    print transformed_image.shape, self.net.blobs['data'].data.shape
    self.net.blobs['data'].data[...] = transformed_image

    #_ = self.net.forward(blobs=self.output_blobs, end='fc7')
    #_ = self.net.forward(blobs=self.output_blobs, start='conv1', end='fc7')
    _ = self.net.forward(blobs=self.output_blobs)
    return self.net.blobs['fc7'].data
    #print output.keys()

    # Return sentibank feature
    # same as self.net.blobs['fc7'].data
    #return output['fc7'][0]



