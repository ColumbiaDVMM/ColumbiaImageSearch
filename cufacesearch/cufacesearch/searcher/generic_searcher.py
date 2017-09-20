from output_mapping import DictOutput
from ..common.conf_reader import ConfReader

default_prefix = "FSE_"


class GenericSearcher(ConfReader):

  def __init__(self, global_conf_in, prefix=default_prefix):
    super(GenericSearcher, self).__init__(global_conf_in, prefix)

    # Initialize attributes
    # TODO: rename model_type in searcher type?
    self.model_type = None
    self.searcher = None
    self.detector_type = None
    self.detector = None
    self.featurizer_type = None
    self.featurizer = None
    self.indexer_type = None
    self.indexer = None
    self.verbose = 1
    self.top_feature = 0

    # Read conf
    self.read_conf()

    self.dict_output_type = self.get_param('dict_output_type')

    # Initialize everything
    self.init_detector()
    self.init_featurizer()
    self.init_indexer()
    self.init_searcher()
    self.init_storer()

    # Initialize dict output for formatting
    if self.dict_output_type:
      self.do = DictOutput(self.dict_output_type)
    else:
      self.do = DictOutput()

    self.url_field = 'info:s3_url'
    self.do.url_field = self.url_field
    # TODO: Also add feature column for re-ranking (can we use prefix filter?)
    self.needed_output_columns = [self.url_field]

    # should codes path be a list to deal with updates?
    # should we store that list in HBase?
    # TODO: load pickled codes files from s3 bucket
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
    self.indexer_type = self.get_required_param('indexer')
    if self.indexer_type == "hbase_indexer_minimal":
      from ..indexer.hbase_indexer_minimal import HBaseIndexerMinimal
      self.indexer = HBaseIndexerMinimal(self.global_conf)
    else:
      raise ValueError("[{}: error] unknown 'indexer' {}.".format(self.pp, self.indexer_type))

  def init_detector(self):
    """ Initialize detector based on 'detector' in 'global_conf' value.
    """
    # A detector is not required
    self.detector_type = self.get_param('detector')
    if self.detector_type:
      from ..detector.generic_detector import get_detector
      self.detector = get_detector(self.detector_type)

  def init_featurizer(self):
    """ Initialize Feature Extractor from `global_conf` value.
    """
    self.featurizer_type = self.get_required_param('featurizer')
    from ..featurizer.generic_featurizer import get_featurizer
    self.featurizer = get_featurizer(self.featurizer_type, self.global_conf)

  def init_storer(self):
    """ Initialize storer from `global_conf` value.
    """
    from ..storer.generic_storer import get_storer
    storer_type = self.get_required_param("storer_type")
    # try to get prefix from conf
    prefix = default_prefix
    tmp_prefix = self.get_param("storer_prefix")
    if tmp_prefix:
      prefix = tmp_prefix
    self.storer = get_storer(storer_type, self.global_conf, prefix=prefix)

  def check_ratio(self):
    '''Check if we need to set the ratio based on top_feature.'''
    if self.top_feature > 0:
      self.ratio = self.top_feature * 1.0 / len(self.searcher.nb_indexed)
      log_msg = "[{}.check_ratio: log] Set ratio to {} as we want top {} images out of {} indexed."
      print log_msg.format(self.pp, self.ratio, self.top_feature, len(self.searcher.nb_indexed))

  def search_image_list(self, image_list, options_dict=dict()):
    # To deal with a featurizer without detection, just pass the imgio 'get_buffer_from_URL' function
    if self.detector is None:
      from ..imgio.imgio import get_buffer_from_URL
      detect_load_fn = get_buffer_from_URL
    else:
      detect_load_fn = self.detector.detect_from_url
    return self._search_from_any_list(image_list, detect_load_fn, options_dict)

  def search_imageB64_list(self, imageB64_list, options_dict=dict()):
    # To deal with a featurizer without detection, just pass the imgio 'get_buffer_from_B64' function
    if self.detector is None:
      from ..imgio.imgio import get_buffer_from_B64
      detect_load_fn = get_buffer_from_B64
    else:
      detect_load_fn = self.detector.detect_from_b64
    return self._search_from_any_list(imageB64_list, detect_load_fn, options_dict)

  def _search_from_any_list(self, image_list, detect_load_fn, options_dict):
    dets = []
    feats = []
    import time
    total_detect = 0.0
    total_featurize = 0.0
    # For each image
    for image in image_list:

      if self.detector is not None:
        # First detect
        start_detect = time.time()
        sha1, img_type, width, height, img, faces = detect_load_fn(image)
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
      else:
        # load image first (it could be either URL or B64...)
        start_featurize = time.time()
        img = detect_load_fn(image)
        img_feat = self.featurizer.featurize(img)
        featurize_time = time.time() - start_featurize
        print 'Featurized one image in {:0.3}s.'.format(featurize_time)
        total_featurize += featurize_time
        feats.append(img_feat)

    # Search from all faces features
    return self.search_from_feats(dets, feats, options_dict)

  # These are the methods you should override for a new searcher
  def init_searcher(self):
    raise NotImplementedError('init_searcher')

  def add_features(self, feats, ids=None):
    raise NotImplementedError('add_features')

  def train_index(self):
    raise NotImplementedError('train_index')

  def save_index(self):
    raise NotImplementedError('save_index')

  def load_index(self):
    raise NotImplementedError('load_index')

  def search_from_feats(self, dets, feats, options_dict=dict()):
    raise NotImplementedError('search_from_feats')
