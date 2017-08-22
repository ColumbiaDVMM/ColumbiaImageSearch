from .generic_kafka_processor import GenericKafkaProcessor
from ..imgio.imgio import get_buffer_from_B64

default_prefix = "KFP_"

class KafkaFaceProcessor(GenericKafkaProcessor):

  def __init__(self, conf, prefix=default_prefix):
    # call GenericKafkaProcessor init (and others potentially)
    super(KafkaFaceProcessor, self).__init__(conf, prefix)
    # any additional initialization needed, like producer specific output logic
    self.face_out_topic = self.get_required_param('face_out_topic')
    self.detector = None
    self.featurizer = None
    self.detector_type = ""
    self.featurizer_type = ""
    self.init_detector()
    self.init_featurizer()
    self.init_indexer()

  def init_detector(self):
    """ Initialize Face Detector from `global_conf` value.
    """
    # Get detector type from conf file
    self.detector_type = self.get_required_param('detector')
    # Get corresponding detector object
    from ..detector.generic_detector import get_detector
    self.detector = get_detector(self.detector_type)

  def init_featurizer(self):
    """ Initialize Feature Extractor from `global_conf` value.
    """
    # Get featurizer type from conf file
    self.featurizer_type = self.get_required_param('featurizer')
    # Get corresponding featurizer object
    from ..featurizer.generic_featurizer import get_featurizer
    self.featurizer = get_featurizer(self.featurizer_type, self.global_conf)

  def init_indexer(self):
    """ Initialize Indexer from `global_conf` value.
    """
    from ..indexer.hbase_indexer_minimal import HBaseIndexerMinimal
    self.indexer = HBaseIndexerMinimal(self.global_conf)

  def set_pp(self):
    self.pp = "KafkaFaceProcessor"

  def init_out_dict(self, sha1):
    tmp_dict_out = dict()
    tmp_dict_out['sha1'] = sha1
    tmp_dict_out['detector_type'] = self.detector_type
    tmp_dict_out['featurizer_type'] = self.featurizer_type
    tmp_dict_out['facefound'] = False
    return tmp_dict_out

  def process_one(self, msg):
    # msg is coming as json with fields: sha1, s3_url, img_infos, img_buffer
    # see 'build_image_msg' of KafkaImageProcessor
    # buffer is B64 encoded and should be decoded with get_buffer_from_B64
    import json
    import base64

    # Check if sha1 is in DB with column 'ext:'+detector_type+'_facefound' set
    # Need to setup a HBaseIndexer...

    # Detect faces
    list_faces_msg = []
    img, dets = self.detector.detect_from_buffer_noinfos(get_buffer_from_B64(msg['img_buffer']), up_sample=1)
    if dets:
      # For each detected face...
      for one_face in dets:
        # Compute face feature
        one_feat = self.featurizer.featurize(img, one_face)
        # Build out dictionary
        tmp_dict_out = self.init_out_dict(msg['sha1'])
        tmp_dict_out['facefound'] = True
        tmp_dict_out['face_bbox'] = one_face
        # base64 encode the feature to be dumped
        tmp_dict_out['face_feat'] = base64.b64encode(one_feat)
        # Dump as JSON
        list_faces_msg.append(json.dumps(tmp_dict_out).encode('utf-8'))
    else:
      # Push one default dict with 'facefound' set to False
      tmp_dict_out = self.init_out_dict(msg['sha1'])
      list_faces_msg.append(json.dumps(tmp_dict_out).encode('utf-8'))

    # Push to face_out_topic
    for face_msg in list_faces_msg:
      self.producer.send(self.face_out_topic, face_msg)
