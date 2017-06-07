from kaffetensorflow.network import Network
import tensorflow as tf
from scipy import misc
import numpy as np
import time
import sys

# lrn is 2, 2e-05, 0.75 when lrn_param is { local_size: 5, alpha: 0.0001, beta: 0.75 } in caffe prototxt
# because the formulas are slightly different. But these two config should produce the same results.
# http://caffe.berkeleyvision.org/tutorial/layers/lrn.html
# https://www.tensorflow.org/api_docs/python/tf/nn/local_response_normalization

padding_type = 'VALID'
imagedltimeout = 2


def get_img_notransform_from_URL_StringIO(url, verbose=0):
    from cStringIO import StringIO
    import requests
    if verbose > 1:
        print "Downloading image from {}.".format(url)
    try:
        r = requests.get(url, timeout=imagedltimeout)
        if r.status_code == 200:
            r_sio = StringIO(r.content)
            if int(r.headers['content-length']) == 0:
                del r
                raise ValueError("Empty image.")
            else:
                img_out = misc.imread(r_sio)
                del r, r_sio
                return img_out
    except Exception as inst:
        print "Download failed from url {}. [{}]".format(url, inst)
    return None


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
        #self.resize_type = 'bilinear'
        # init mean image
        self.imgmean = np.load(imgmeanpath)
        self.imgmean = np.swapaxes(self.imgmean, 0, 2)
        self.net = None
        self.init_net()

    # setup network
    def init_net(self):
        print "[init_net] Loading DeepSentibank net..."
        sys.stdout.flush()
        start_init = time.time()
        self.input_data = tf.placeholder(tf.float32, shape=self.net_input_size)
        self.session.run(tf.global_variables_initializer())
        self.net = DeepSentibankNet({'data': self.input_data}, trainable=False)
        # Load the data
        self.net.load(self.modelpath, self.session)
        print "[init_net] Loaded DeepSentibank net in {}s".format(time.time() - start_init)


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


    # preprocess image
    def get_deepsentibank_preprocessed_img_from_URL(self, url, verbose=0):
        # could we use boto3?
        img_out = get_img_notransform_from_URL_StringIO(url, verbose)
        if img_out is not None:
            img_out = self.preprocess_img(img_out)
        return img_out


    # extract from URL
    def get_features_from_URL(self, url, features=['fc7'], verbose=False):
        img = self.get_deepsentibank_preprocessed_img_from_URL(url)
        return self.get_features_from_img(img, features, verbose)


    # extract from image filename
    def get_features_from_img_filename(self, img_filename, features=['fc7'], verbose=False):
        img = self.preprocess_img(misc.imread(img_filename))
        return self.get_features_from_img(img, features, verbose)


    # extract from image
    def get_features_from_img(self, img, features=['fc7'], verbose=False):
        output = self.session.run(self.net.get_output(out_layers=features), feed_dict={self.input_data: np.asarray([img.astype("float32")])})
        return output