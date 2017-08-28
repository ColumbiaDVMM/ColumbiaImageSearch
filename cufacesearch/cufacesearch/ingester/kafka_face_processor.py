import os
import sys
import time
import json
import multiprocessing
from .generic_kafka_processor import GenericKafkaProcessor
from ..imgio.imgio import get_buffer_from_B64
from ..featurizer.featsio import featB64encode
from ..indexer.hbase_indexer_minimal import str_processed

default_prefix = "KFP_"
key_str = 'sha1'
face_feat_str = 'face_feat'
face_bbox_str = 'face_bbox'

class KafkaFaceProcessor(GenericKafkaProcessor):

  def __init__(self, conf, prefix=default_prefix, pid=None):
    # when running as deamon
    self.pid = pid
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
    self.face_extr_prefix = "ext:"+"_".join([self.featurizer_type, "feat", self.detector_type, "face"])

  def set_pp(self):
    self.pp = "KafkaFaceProcessor"
    if self.pid:
      self.pp += ":"+str(self.pid)

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

  def init_out_dict(self, sha1):
    tmp_dict_out = dict()
    tmp_dict_out[key_str] = sha1
    tmp_dict_out[str_processed] = False
    # This should be used to mark this image as processed by this combination detector/featurizer
    # e.g. pushing at least a column 'dlib_feat_dlib_face_facefound' with value False
    # features should be pushed as B64 encoded in a column 'dlib_feat_dlib_face_BBOX_SCORE',
    # BBOX order should be: left, top, right, bottom. See workflows/push_facedata_to_hbase.py
    #tmp_dict_out['face_extr_prefix'] = self.face_extr_prefix
    tmp_dict_out['detector_type'] = self.detector_type
    tmp_dict_out['featurizer_type'] = self.featurizer_type
    return tmp_dict_out

  def get_bbox_str(self, bbox):
    # Build bbox string as left_top_right_bottom_score
    return "_".join(["{}"]*5).format(bbox["left"], bbox["top"], bbox["right"], bbox["bottom"], bbox["score"])

  def build_hbase_dict(self, list_faces_msg):
    dict_rows = dict()
    for msg_str in list_faces_msg:
      msg = json.loads(msg_str)
      dict_rows[msg[key_str]] = dict()
      dict_rows[msg[key_str]][self.face_extr_prefix+"_"+str_processed] = str(int(msg[str_processed]))
      if face_bbox_str in msg and face_feat_str in msg:
        dict_rows[msg[key_str]][self.face_extr_prefix + "_" + self.get_bbox_str(msg[face_bbox_str])] = msg[face_feat_str]
    return dict_rows


  def process_one(self, full_msg):
    start_process = time.time()
    # msg is coming as json with fields: sha1, s3_url, img_infos, img_buffer
    # see 'build_image_msg' of KafkaImageProcessor
    # buffer is B64 encoded and should be decoded with get_buffer_from_B64
    try:
      self.print_stats(full_msg)
      msg = json.loads(full_msg.value)
      #print msg

      # Check if sha1 is in DB with column 'ext:'+feature_type,detector_type+'_processed' set for row msg['sha1']
      check_column = self.indexer.get_check_column(self.face_extr_prefix)
      rows = self.indexer.get_columns_from_sha1_rows([msg[key_str]], [check_column], families={'info': dict(), 'ext': dict()})
      if rows:
        # we should skip
        self.toc_process_skip(start_process)
        return

      # Detect faces
      list_faces_msg = []
      img, dets = self.detector.detect_from_buffer_noinfos(get_buffer_from_B64(msg['img_buffer']), up_sample=1)
      if dets:
        # For each detected face...
        for one_face in dets:
          # Compute face feature
          one_feat = self.featurizer.featurize(img, one_face)
          # Build out dictionary
          tmp_dict_out = self.init_out_dict(msg[key_str])
          tmp_dict_out[str_processed] = True
          tmp_dict_out[face_bbox_str] = one_face
          # base64 encode the feature to be dumped
          tmp_dict_out[face_feat_str] = featB64encode(one_feat)
          # Dump as JSON
          list_faces_msg.append(json.dumps(tmp_dict_out).encode('utf-8'))
      else:
        # Push one default dict with 'facefound' set to False
        tmp_dict_out = self.init_out_dict(msg[key_str])
        list_faces_msg.append(json.dumps(tmp_dict_out).encode('utf-8'))

      # Push to face_out_topic
      for face_msg in list_faces_msg:
        self.producer.send(self.face_out_topic, face_msg)

      # Should we push to DB here too? Using push_dict_rows
      self.indexer.push_dict_rows(self.build_hbase_dict(list_faces_msg), table_name=self.indexer.table_sha1infos_name)

      self.toc_process_ok(start_process)
    except Exception as inst:
      self.toc_process_failed(start_process)
      exc_type, exc_obj, exc_tb = sys.exc_info()
      fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
      raise type(inst)("{} {}:{}, {}".format(self.pp, fname, exc_tb.tb_lineno, inst))

class DaemonKafkaFaceProcessor(multiprocessing.Process):

  daemon = True

  def __init__(self, conf, prefix=default_prefix):
    super(DaemonKafkaFaceProcessor, self).__init__()
    self.conf = conf
    self.prefix = prefix

  def run(self):

    try:
      print "Starting worker KafkaFaceProcessor.{}".format(self.pid)
      kp = KafkaFaceProcessor(self.conf, prefix=self.prefix, pid=self.pid)
      for msg in kp.consumer:
        kp.process_one(msg)
    except Exception as inst:
      exc_type, exc_obj, exc_tb = sys.exc_info()
      fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
      print "KafkaFaceProcessor.{} died (In {}:{}, {}:{})".format(self.pid, fname, exc_tb.tb_lineno, type(inst), inst)