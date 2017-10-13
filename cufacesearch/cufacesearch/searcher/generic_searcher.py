from output_mapping import DictOutput
from ..common.conf_reader import ConfReader

default_prefix = "GESEARCH_"


class GenericSearcher(ConfReader):

  def __init__(self, global_conf_in, prefix=default_prefix):
    super(GenericSearcher, self).__init__(global_conf_in, prefix)

    # Initialize attributes default values
    self.model_params = dict()
    self.input_type = "image"
    self.searcher = None
    self.detector_type = "full"
    self.detector = None
    self.featurizer_type = None
    self.featurizer = None
    self.indexer_type = None
    self.indexer = None
    self.model_str = None
    self.extr_str = None
    self.verbose = 1
    self.top_feature = 0
    self.nb_train = 1000000
    # Do re-ranking reading features from HBase? How many features should be read? 1000?
    self.reranking = False
    self.indexed_updates = set()
    self.url_field = 'info:s3_url'

    # TODO: Also add feature column for re-ranking (can we use prefix filter?)
    self.needed_output_columns = [self.url_field]

    # Initialize attributes from conf
    # TODO: rename model_type in searcher type?
    self.model_type = self.get_required_param('model_type')
    self.dict_output_type = self.get_param('dict_output_type')
    # Add any new parameters, e.g. reranking
    self.get_model_params()

    # Have some parameters to discard images of dimensions lower than some values?...
    # Have some parameters to discard detections with scores lower than some values?...

    # Initialize dict output for formatting
    if self.dict_output_type:
      self.do = DictOutput(self.dict_output_type)
    else:
      self.do = DictOutput()
    self.do.url_field = self.url_field

    # Initialize everything
    self.init_detector()
    self.init_featurizer()
    self.init_storer()
    self.init_indexer()
    self.init_searcher()

    # Test the performance of the trained model?
    # To try to set max_returned to achieve some target performance

    # should codes path be a list to deal with updates?
    # should we store that list in HBase?
    # TODO: load pickled codes files from s3 bucket
    self.load_codes()

  def read_conf(self):
    # these parameters may be overwritten by web call
    self.sim_limit = self.get_param('sim_limit')
    if self.sim_limit is None:
      self.sim_limit = 100
    tmp_quota = self.get_param('quota')
    if tmp_quota:
      if tmp_quota < self.sim_limit:
        raise ValueError("'quota' cannot be less than 'sim_limit'")
      self.quota = tmp_quota
    else:
      self.quota = self.sim_limit * 10
    self.near_dup = bool(self.get_param('near_dup'))
    self.near_dup_th = self.get_param('near_dup_th')
    self.ratio = self.get_param('ratio')
    tmp_top_feature = self.get_param('top_feature')
    if tmp_top_feature:
      self.top_feature = int(tmp_top_feature)
    tmp_input_type = self.get_param('input_type')
    if tmp_input_type:
      self.input_type = tmp_input_type
    tmp_nb_train = self.get_param('nb_train')
    if tmp_nb_train:
      self.nb_train = tmp_nb_train

  def get_model_params(self):
    raise NotImplementedError("[{}] get_model_params is not implemented".format(self.pp))

  def get_model_params_str(self):
    model_params_str = ''
    for p in self.model_params:
      model_params_str += "-"+str(p)+str(self.model_params[p])
    model_params_str += '_train{}'.format(self.nb_train)
    return model_params_str


  def build_extr_str(self):
    if self.extr_str is None:
      # use generic extractor 'build_extr_str'
      from cufacesearch.extractor.generic_extractor import build_extr_str
      # featurizer_type, detector_type, input_type):
      self.extr_str = build_extr_str(self.featurizer_type, self.detector_type, self.input_type)
    return self.extr_str

  def get_train_features_str(self):
    extr_str = self.build_extr_str()
    return "train_features_{}_{}.pkl".format(extr_str, self.nb_train)

  def build_model_str(self):
    model_params_str = self.get_model_params_str()
    if self.model_str is None:
      # We could add some additional info, like model parameters, number of samples used for training...
      self.model_str = self.build_extr_str() + "_" + self.model_type + model_params_str
    return self.model_str

  def build_codes_string(self, update_id):
    model_string = self.build_model_str()
    return model_string+"_codes/"+update_id

  def init_indexer(self):
    """ Initialize HBase Indexer from `global_conf` value.
    """
    # Get indexed type from conf file
    self.indexer_type = self.get_required_param('indexer_type')
    tmp_prefix = self.get_param("indexer_prefix")
    if self.indexer_type == "hbase_indexer_minimal":
      from ..indexer.hbase_indexer_minimal import HBaseIndexerMinimal, default_prefix as hbi_default_prefix
      prefix = hbi_default_prefix
      if tmp_prefix:
        prefix = tmp_prefix
      self.indexer = HBaseIndexerMinimal(self.global_conf, prefix=prefix)
    else:
      raise ValueError("[{}: error] unknown 'indexer' {}.".format(self.pp, self.indexer_type))

  def init_detector(self):
    """ Initialize detector based on 'detector' in 'global_conf' value.
    """
    # A detector is not required
    detector_type = self.get_param('detector_type')
    if detector_type:
      self.detector_type = detector_type
      if self.detector_type != "full":
        from ..detector.generic_detector import get_detector
        self.detector = get_detector(self.detector_type)

  def init_featurizer(self):
    """ Initialize Feature Extractor from `global_conf` value.
    """
    self.featurizer_type = self.get_required_param('featurizer_type')
    tmp_prefix = self.get_param("featurizer_prefix")
    from ..featurizer.generic_featurizer import get_featurizer
    self.featurizer = get_featurizer(self.featurizer_type, self.global_conf, tmp_prefix)

  def init_storer(self):
    """ Initialize storer from `global_conf` value.
    """
    from ..storer.generic_storer import get_storer, default_prefix as storer_default_prefix
    storer_type = self.get_required_param("storer_type")
    # try to get prefix from conf
    prefix = storer_default_prefix
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
      detect_load_fn = lambda x: get_buffer_from_URL(x)
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
    # TODO: check if we have the "data:image/jpeg;base64," at the beggining of each B64 image?
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
        from ..imgio.imgio import get_SHA1_from_buffer
        start_featurize = time.time()
        img = detect_load_fn(image)
        sha1 = get_SHA1_from_buffer(img)
        # Still fill a dets list with the image sha1 to propagate down for the search results...
        if image.startswith('http'):
          dets.append((sha1, image))
        else:
          dets.append((sha1, None))
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
