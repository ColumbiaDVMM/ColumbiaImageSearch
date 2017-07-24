import json
import numpy as np

from output_mapping import DictOutput

class GenericSearcher():

  def __init__(self, global_conf_filename, prefix="FSE_"):
    self.global_conf_filename = global_conf_filename
    self.prefix = prefix
    self.global_conf = json.load(open(global_conf_filename, 'rt'))
    self.pp = ""
    self.set_pp()

    # Initialize attributes
    self.searcher = None
    self.verbose = 1
    self.top_feature = 0

    # Read conf and initialize everything
    self.read_conf()
    self.init_detector()
    self.init_featurizer()
    self.init_hbaseindexer()
    self.init_searcher()

    # Initialize dict output for formatting
    dict_output_type = self.get_param('dict_output_type')
    if dict_output_type:
      self.do = DictOutput(dict_output_type)
    else:
      self.do = DictOutput()

    self.url_field = 'info:s3_url'
    self.do.url_field = self.url_field
    self.needed_output_columns = [self.url_field]

    # should codes path be a list to deal with updates?
    # should we store that list in HBase?
    self.codes_path = self.get_param('codes_path')
    self.load_codes()

  def get_param(self, param):
    key_param = self.prefix + param
    if key_param in self.global_conf:
      return self.global_conf[key_param]
    if self.verbose:
      print '[{}.get_param: info] could not find {} in configuration'.format(self.pp, key_param)

  def read_conf(self):
    # these parameters may be overwritten by web call
    self.sim_limit = self.get_param('sim_limit')
    self.quota = self.sim_limit * 10
    self.near_dup = bool(self.get_param('near_dup'))
    self.near_dup_th = self.get_param('near_dup_th')
    self.ratio = self.get_param('ratio')
    tmp_top_feature = self.get_param('top_feature')
    if tmp_top_feature:
      self.top_feature = int(tmp_top_feature)

  def init_hbaseindexer(self):
    """ Initialize HBase Indexer from `global_conf` value.
    """
    # Get indexed type from conf file
    indexer_type = self.get_param('indexer')
    if not indexer_type:
      raise ValueError("[{}: error] 'indexer' is not defined in configuration file.".format(self.pp))
    elif indexer_type == "hbase_indexer_minimal":
      from ..indexer.hbase_indexer_minimal import HBaseIndexerMinimal
      self.indexer = HBaseIndexerMinimal(self.global_conf_filename)
    else:
      raise ValueError("[{}: error] unkown 'indexer' {}.".format(self.pp, indexer_type))

  def init_detector(self):
    """ Initialize Face Detector from `global_conf` value.
    """
    # Get indexed type from conf file
    detector_type = self.get_param('detector')
    if not detector_type:
      raise ValueError("[{}: error] 'detector' is not defined in configuration file.".format(self.pp))
    elif detector_type == "dblib_detector":
      from ..detector import dblib_detector
      self.detector = dblib_detector.DLibFaceDetector()
    else:
      raise ValueError("[{}: error] unkown 'detector' {}.".format(self.pp, detector_type))

  def init_featurizer(self):
    """ Initialize Feature Extractor from `global_conf` value.
    """
    featurizer_type = self.get_param('featurizer')
    if not featurizer_type:
      raise ValueError("[{}: error] 'featurizer' is not defined in configuration file.".format(self.pp))
    elif featurizer_type == "dlib_featurizer":
      from ..featurizer.dblib_featurizer import DLibFeaturizer
      self.featurizer = DLibFeaturizer(self.global_conf_filename)
    else:
      raise ValueError("[{}: error] unkown 'featurizer' {}.".format(self.pp, featurizer_type))

  def check_ratio(self):
    '''Check if we need to set the ratio based on top_feature.'''
    if self.top_feature > 0:
      self.ratio = self.top_feature * 1.0 / len(self.searcher.nb_indexed)
      log_msg = "[{}.check_ratio: log] Set ratio to {} as we want top {} images out of {} indexed."
      print log_msg.format(self.pp, self.ratio, self.top_feature, len(self.searcher.nb_indexed))

  def search_image_list(self, image_list, options_dict=dict()):
    return self._search_from_any_list(image_list, self.detector.detect_from_url, options_dict)

  def search_imageB64_list(self, imageB64_list, options_dict=dict()):
    return self._search_from_any_list(imageB64_list, self.detector.detect_from_b64, options_dict)

  def _search_from_any_list(self, image_list, detect_fn, options_dict):
    dets = []
    feats = []
    # for each image
    for image in image_list:
      # first detect faces
      sha1, img_type, width, height, img, faces = detect_fn(image)
      if image.startswith('http'):
        dets.append((sha1, faces, image, img_type, width, height))
      else:
        dets.append((sha1, faces, None, img_type, width, height))
      # if we found faces, get features for each face
      faces_feats = []
      # check if we were asked only to perform detection
      if "detect_only" not in options_dict or not options_dict["detect_only"]:
        for one_face in faces:
          print one_face
          one_feat = self.featurizer.featurize(img, one_face)
          faces_feats.append(one_feat)
      feats.append(faces_feats)

    # search from all faces features
    return self.search_from_feats(dets, feats, options_dict)

  # These are the 3 methods you should override for a new searcher
  def init_searcher(self):
    raise NotImplementedError('init_searcher')

  def load_codes(self):
    raise NotImplementedError('load_codes')

  def search_from_feats(self, dets, feats, options_dict=dict()):
    raise NotImplementedError('search_from_feats')
