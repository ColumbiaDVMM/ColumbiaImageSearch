from .generic_kafka_processor import GenericKafkaProcessor

default_prefix = "KFP_"

class KafkaFaceProcessor(GenericKafkaProcessor):

  def __init__(self, conf, prefix=default_prefix):
    # call GenericKafkaProcessor init (and others potentially)
    super(KafkaFaceProcessor, self).__init__(conf, prefix)
    # any additional initialization needed, like producer specific output logic
    self.face_out_topic = self.get_required_param('face_out_topic')
    self.detector = None
    self.featurizer = None
    self.init_detector()
    self.init_featurizer()

  def init_detector(self):
    """ Initialize Face Detector from `global_conf` value.
    """
    # Get indexed type from conf file
    detector_type = self.get_required_param('detector')
    from ..detector.generic_detector import get_detector
    self.detector = get_detector(detector_type)

  def init_featurizer(self):
    """ Initialize Feature Extractor from `global_conf` value.
    """
    featurizer_type = self.get_required_param('featurizer')
    # should be featurizer factory?
    from ..featurizer.generic_featurizer import get_featurizer
    self.featurizer = get_featurizer(featurizer_type, self.global_conf_filename)


  def set_pp(self):
    self.pp = "KafkaFaceProcessor"

  def process_one(self, msg):
    # msg is coming as tuple (sha1, s3_url, img_infos, img_buffer)

    # Detect faces and featurize each face
    list_faces_msg = []
    # should we upsample or not?
    img, dets = self.detector.detect_from_buffer_noinfos(msg[3], up_sample=0)
    for one_face in dets:
      one_feat = self.featurizer.featurize(img, one_face)
      # should we base64 encode the feature?
      list_faces_msg.append((msg[0], one_face, one_feat),)

    # Push to face_out_topic
    for face_msg in list_faces_msg:
      self.producer.send(self.face_out_topic, face_msg)
