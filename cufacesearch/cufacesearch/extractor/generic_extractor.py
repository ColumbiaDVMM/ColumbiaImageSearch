import sys
import time
import threading
import multiprocessing
from cufacesearch.detector.generic_detector import get_detector, get_bbox_str
from cufacesearch.featurizer.generic_featurizer import get_featurizer
from cufacesearch.featurizer.featsio import featB64encode
from cufacesearch.imgio.imgio import get_buffer_from_B64
from cufacesearch.indexer.hbase_indexer_minimal import extr_str_processed


def build_extr_str(featurizer_type, detector_type, input_type):
  return "_".join([featurizer_type, "feat", detector_type, input_type])


def build_extr_str_processed(featurizer_type, dectector_type, input_type):
  return build_extr_str(featurizer_type, dectector_type, input_type)+"_"+extr_str_processed


class ThreadedExtractor(threading.Thread):
  def __init__(self, extractor, q_in, q_out):
    threading.Thread.__init__(self)
    self.extractor = extractor
    self.q_in = q_in
    self.q_out = q_out

  def run(self):
    while self.q_in.empty() == False:
      try:
        # The queue should already have items, no need to block
        sha1, img_buffer = self.q_in.get(False)
      except:
        continue

      out_dict = self.extractor.process_buffer(img_buffer)

      # Push
      self.q_out.put((sha1, out_dict))

      # Mark as done
      self.q_in.task_done()


class BatchThreadedExtractor(threading.Thread):
  def __init__(self, extractor, q_in, q_out):
    threading.Thread.__init__(self)
    self.extractor = extractor
    self.q_in = q_in
    self.q_out = q_out

  def run(self):
    while self.q_in.empty() == False:
      try:
        # The queue should already have items, no need to block
        batch = self.q_in.get(False)
      except:
        continue

      start_process = time.time()

      print "[BatchThreadedExtractor] Got batch of {} images to process.".format(len(batch))
      sys.stdout.flush()
      out_batch = []

      for sha1, img_buffer in batch:
        out_dict = self.extractor.process_buffer(img_buffer)
        out_batch.append((sha1, out_dict))

      # Push
      print "[BatchThreadedExtractor] Computed {} extractions in {}s.".format(len(out_batch), time.time() - start_process)
      sys.stdout.flush()
      self.q_out.put(out_batch)

      # Mark as done
      self.q_in.task_done()


class DaemonBatchExtractor(multiprocessing.Process):

  daemon = True

  def __init__(self, extractor, q_in, q_out, verbose=0):
    super(DaemonBatchExtractor, self).__init__()
    self.extractor = extractor
    self.q_in = q_in
    self.q_out = q_out
    self.verbose = verbose

  def run(self):
    while self.q_in.empty() == False:
      try:
        # The queue should already have items, no need to block
        batch = self.q_in.get(False)
      except:
        continue

      start_process = time.time()

      if self.verbose > 1:
        print "[DaemonBatchExtractor.{}] Got batch of {} images to process.".format(self.pid, len(batch))
      sys.stdout.flush()
      out_batch = []

      for sha1, img_buffer_b64 in batch:
        out_dict = self.extractor.process_buffer(get_buffer_from_B64(img_buffer_b64))
        out_batch.append((sha1, out_dict))

      # Push
      if self.verbose > 0:
        print "[DaemonBatchExtractor.{}] Computed {} extractions in {}s.".format(self.pid, len(out_batch),
                                                                               time.time() - start_process)
      sys.stdout.flush()
      self.q_out.put(out_batch)

      # Mark as done
      self.q_in.task_done()


class GenericExtractor(object):

  def __init__(self, detector_type, featurizer_type, input_type, extr_column, extr_prefix, global_conf):
    self.detector_type = detector_type
    self.featurizer_type = featurizer_type
    self.input_type = input_type
    self.extr_column = extr_column
    self.global_conf = global_conf
    self.detector = get_detector(self.detector_type)
    self.featurizer = get_featurizer(self.featurizer_type, self.global_conf, prefix=extr_prefix)
    self.extr_str = str(self.extr_column+":"+build_extr_str(self.featurizer_type, self.detector_type, self.input_type))
    self.extr_str_processed = str(self.extr_column+":"+build_extr_str_processed(self.featurizer_type,
                                                                                self.detector_type, self.input_type))

  def init_out_dict(self):
    tmp_dict_out = dict()
    # Will stay '0' for an extractor with a detector where no detection were found
    tmp_dict_out[self.extr_str_processed] = 0
    return tmp_dict_out

  def process_buffer(self, img_buffer):
    dict_out = self.init_out_dict()
    # If extraction needs detection first
    if self.detector is not None:
      img, dets = self.detector.detect_from_buffer_noinfos(img_buffer, up_sample=1)
      if dets:
        # For each detected object/face...
        for one_det in dets:
          # Compute detection feature
          one_feat = self.featurizer.featurize(img, one_det)
          # Fill out dictionary
          dict_out[self.extr_str_processed] = 1
          bbox_str = get_bbox_str(one_det)
          # Encode the feature with base64
          dict_out[self.extr_str + "_" + bbox_str] = featB64encode(one_feat)
    # Just featurize full image
    else:
      one_feat = self.featurizer.featurize(img_buffer)
      dict_out[self.extr_str] = featB64encode(one_feat)
      dict_out[self.extr_str_processed] = 1

    dict_out[self.extr_str_processed] = str(dict_out[self.extr_str_processed])
    # Return dict ready to be pushed to DB
    return dict_out
