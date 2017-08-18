from output_mapping import DictOutput
from ..common.conf_reader import ConfReader

class GenericSearcher(ConfReader):

  def __init__(self, global_conf_in, prefix="FSE_"):
    super(GenericSearcher, self).__init__(global_conf_in, prefix)

    # Initialize attributes
    self.searcher = None
    self.verbose = 1
    self.top_feature = 0

    # Read conf
    self.read_conf()

    self.codes_path = self.get_param('codes_path')
    self.dict_output_type = self.get_param('dict_output_type')

    # Initialize everything
    self.init_detector()
    self.init_featurizer()
    self.init_indexer()
    self.init_searcher()

    # Initialize dict output for formatting
    if self.dict_output_type:
      self.do = DictOutput(self.dict_output_type)
    else:
      self.do = DictOutput()

    self.url_field = 'info:s3_url'
    self.do.url_field = self.url_field
    self.needed_output_columns = [self.url_field]

    # should codes path be a list to deal with updates?
    # should we store that list in HBase?
    self.load_codes()

  def read_conf(self):
    # these parameters may be overwritten by web call
    self.sim_limit = self.get_param('sim_limit')
    self.quota = self.sim_limit * 2
    self.near_dup = bool(self.get_param('near_dup'))
    self.near_dup_th = self.get_param('near_dup_th')
    self.ratio = self.get_param('ratio')
    tmp_top_feature = self.get_param('top_feature')
    if tmp_top_feature:
      self.top_feature = int(tmp_top_feature)

  def init_indexer(self):
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
      raise ValueError("[{}: error] unknown 'indexer' {}.".format(self.pp, indexer_type))

  def init_detector(self):
    """ Initialize Face Detector from `global_conf` value.
    """
    detector_type = self.get_param('detector')
    from ..detector.generic_detector import get_detector
    self.detector = get_detector(detector_type)

  def init_featurizer(self):
    """ Initialize Feature Extractor from `global_conf` value.
    """
    featurizer_type = self.get_param('featurizer')
    from ..featurizer.generic_featurizer import get_featurizer
    self.featurizer = get_featurizer(featurizer_type, self.global_conf)

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
    import time
    total_detect = 0.0
    total_featurize = 0.0
    # For each image
    for image in image_list:
      # First detect faces
      start_detect = time.time()
      sha1, img_type, width, height, img, faces = detect_fn(image)
      detect_time = time.time() - start_detect
      print 'Detect in one image in {:0.3}s.'.format(detect_time)
      total_detect += detect_time
      if image.startswith('http'):
        dets.append((sha1, faces, image, img_type, width, height))
      else:
        dets.append((sha1, faces, None, img_type, width, height))
      # If we found faces, get features for each face
      faces_feats = []
      # Check if we were asked only to perform detection
      if "detect_only" not in options_dict or not options_dict["detect_only"]:
        for one_face in faces:
          print one_face
          start_featurize = time.time()
          one_feat = self.featurizer.featurize(img, one_face)
          featurize_time = time.time() - start_featurize
          print 'Featurized one face in {:0.3}s.'.format(featurize_time)
          total_featurize += featurize_time
          faces_feats.append(one_feat)
      feats.append(faces_feats)

    # Search from all faces features
    return self.search_from_feats(dets, feats, options_dict)

  # These are the 3 methods you should override for a new searcher
  def init_searcher(self):
    raise NotImplementedError('init_searcher')

  def load_codes(self):
    raise NotImplementedError('load_codes')

  def search_from_feats(self, dets, feats, options_dict=dict()):
    raise NotImplementedError('search_from_feats')
