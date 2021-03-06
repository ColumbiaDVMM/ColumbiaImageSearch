"""The classes in that module can process image with any valid combination of detector and
featurizer."""

import gc
import sys
import time
from datetime import datetime
import traceback
import multiprocessing
from ..detector.utils import get_detector, get_bbox_str
from ..featurizer.generic_featurizer import get_featurizer
from ..featurizer.featsio import normfeatB64encode, get_feat_dtype
from ..imgio.imgio import get_buffer_from_B64
from ..indexer.hbase_indexer_minimal import EXTR_STR_PROCESSED, EXTR_STR_FAILED


def build_extr_str(featurizer_type, detector_type, input_type):
  return "_".join([featurizer_type, "feat", detector_type, input_type])

def build_extr_str_processed(featurizer_type, dectector_type, input_type):
  return build_extr_str(featurizer_type, dectector_type, input_type) + "_" + EXTR_STR_PROCESSED

def build_extr_str_failed(featurizer_type, dectector_type, input_type):
  return build_extr_str(featurizer_type, dectector_type, input_type) + "_" + EXTR_STR_FAILED

class DaemonBatchExtractor(multiprocessing.Process):
  """Daemon extractor."""

  daemon = True

  def __init__(self, extractor, q_in, q_out, verbose=0):
    """Daemon extractor constructor.

    :param extractor: extractor object
    :param q_in: input queue
    :param q_out: output queue
    :param verbose: verbose level (default=0)
    """
    super(DaemonBatchExtractor, self).__init__()
    self.extractor = extractor
    self.pp_str = build_extr_str(self.extractor.featurizer_type, self.extractor.detector_type, self.extractor.input_type)
    self.pp = "DaemonBatchExtractor.{}".format(self.pp_str)
    self.q_in = q_in
    self.q_out = q_out
    self.verbose = verbose
    self.qin_timeout = 5
    self.killed = False

  def run(self):
    """Process input queue and push results to output queue.
    """
    empty = False
    self.pp = "DaemonBatchExtractor.{}.{}".format(self.pp_str, self.pid)
    # Unreliable...
    #while self.q_in.empty() == False:
    while not empty:

      if self.killed:
        if self.verbose > 2:
          print "[{}] Killed at: {}".format(self.pp, datetime.now().isoformat())
          sys.stdout.flush()
        break

      try:
        # The queue should already have items,but seems sometime to block forever...
        #batch = self.q_in.get(False)
        if self.verbose > 5:
          print "[{}] Looking for a batch at: {}".format(self.pp, datetime.now().isoformat())
          sys.stdout.flush()
        batch = self.q_in.get(timeout=self.qin_timeout)
      except Exception:
        # This may appear in the log when the following update is being processed.
        if self.verbose > 6:
          print "[{}] Did not get a batch. Leaving".format(self.pp)
          sys.stdout.flush()
        empty = True
        continue

      try:

        start_process = time.time()
        batch_size = len(batch)
        if self.verbose > 1:
          print "[{}] Got batch of {} images to process.".format(self.pp, batch_size)
          sys.stdout.flush()

        # Process each image
        #---
        # Something in this part seems to block with sbpycaffe but very rarely...
        out_batch = []
        #for sha1, img_buffer_b64, push_buffer in batch:
        for img_id in range(batch_size):
          if self.killed:
            if self.verbose > 2:
              print "[{}] Killed at: {}".format(self.pp, datetime.now().isoformat())
              sys.stdout.flush()
            break
          sha1, img_buffer_b64, push_buffer = batch.pop()
          try:
            # Could this block???
            out_dict = self.extractor.process_buffer(get_buffer_from_B64(img_buffer_b64))
            # We have downloaded the image and need to push the buffer to HBase
            # Transition: we should never push back.
            # Also buffer is really buffer not b64 encoded...
            # if push_buffer:
            #   #out_dict[img_buffer_column] = img_buffer_b64
            #   out_dict[self.indexer.get_col_imgbuff()] = img_buffer_b64
            out_batch.append((sha1, out_dict))
          except Exception as inst:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fulltb = traceback.format_tb(exc_tb)
            #err_msg = "[{}: warning] Extraction failed for img {} with error ({}): {} {}"
            #print(err_msg.format(self.pp, sha1, type(inst), inst, fulltb))
            err_msg = "[{}: warning] Extraction failed for img #{} {} with error ({}): {} {}"
            print(err_msg.format(self.pp, img_id, sha1, type(inst), inst, fulltb))
            sys.stdout.flush()
            # len(img_buffer_b64)
            try:
              err_msg = "[{}: warning] Extraction failed for img #{} {}. img_buffer_b64 length was {}"
              print(err_msg.format(self.pp, img_id, sha1, len(img_buffer_b64)))
              sys.stdout.flush()
            except:
              pass
            # Mark this image as corrupted
            # But how and when could it be overwritten if we manage to run the extraction later?
            out_dict = self.extractor.failed_out_dict()
            out_batch.append((sha1, out_dict))
        #---

        # Push batch out
        if self.verbose > 0:
          print_msg = "[{}] Computed {}/{} extractions in {}s."
          print print_msg.format(self.pp, len(out_batch), batch_size, time.time() - start_process)
          sys.stdout.flush()

        # Put with block but allow timeout
        # Still seems to block forever if not enough memory is available? Cleanup first
        del batch
        gc.collect()
        self.q_out.put(out_batch, True, 10)

        # Mark as done
        self.q_in.task_done()

      except Exception as inst:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fulltb = traceback.format_tb(exc_tb)
        print("[{}: {}] {} ({})".format(self.pp, type(inst), inst, ''.join(fulltb)))
        sys.stdout.flush()

        # Try to push whatever we have so far?
        #if out_batch:
        #  self.q_out.put(out_batch)

        # Try to mark as done anyway?
        # This can make things worst if error is task_done() called too many times...
        #self.q_in.task_done()

    # Cleanup without waiting for GC
    del self.extractor
    #print "[DaemonBatchExtractor.{}] Reached end of input queue at: {}".format(self.pid, datetime.now().isoformat())
    #sys.stdout.flush()


class GenericExtractor(object):
  """GenericExtractor class.
  """

  def __init__(self, detector_type, featurizer_type, input_type, extr_column, extr_prefix,
               global_conf):
    """Extractor constructor.

    :param detector_type: detector type
    :type detector_type: str
    :param featurizer_type: featurizer type
    :type featurizer_type: str
    :param input_type: input type
    :type input_type: str
    :param extr_column: extraction column
    :type extr_column: str
    :param extr_prefix: extraction prefix
    :type extr_prefix: str
    :param global_conf: configuration filename or dictionary
    :type global_conf: str, dict
    """
    self.detector_type = detector_type
    self.featurizer_type = featurizer_type
    self.input_type = input_type
    self.extr_column = extr_column
    self.global_conf = global_conf
    self.detector = get_detector(self.detector_type)
    self.featurizer = get_featurizer(self.featurizer_type, self.global_conf, prefix=extr_prefix)
    tmp_str = build_extr_str(self.featurizer_type, self.detector_type, self.input_type)
    self.extr_str = str(self.extr_column + ":" + tmp_str)
    tmp_str = build_extr_str_processed(self.featurizer_type, self.detector_type, self.input_type)
    self.extr_str_processed = str(self.extr_column + ":" + tmp_str)
    tmp_str = build_extr_str_failed(self.featurizer_type, self.detector_type, self.input_type)
    self.extr_str_failed = str(self.extr_column + ":" + tmp_str)

  def init_out_dict(self):
    """Create output dictionary.

    :return: dictionary
    :rtype: dict
    """
    tmp_dict_out = dict()
    # Will stay '0' for an extractor with a detector where no detection were found
    tmp_dict_out[self.extr_str_processed] = str(0)
    return tmp_dict_out

  def failed_out_dict(self):
    """Create failed dictionary.

    :return: dictionary
    :rtype: dict
    """
    tmp_dict_out = dict()
    tmp_dict_out[self.extr_str_failed] = str(1)
    return tmp_dict_out

  def process_buffer(self, img_buffer):
    """Process image buffer.

    :param img_buffer: image buffer
    :type img_buffer: :class:`numpy.ndarray`
    :return: dictionary
    :rtype: dict
    """
    dict_out = self.init_out_dict()

    # If extraction needs detection first
    if self.detector is not None:
      img, dets = self.detector.detect_from_buffer_noinfos(img_buffer, up_sample=1)
      if dets:
        # For each detected object/face...
        for one_det in dets:
          # Compute detection feature
          one_feat = self.featurizer.featurize(img, one_det)
          # force type to be as expected
          one_feat = one_feat.astype(get_feat_dtype(self.featurizer_type))
          # Fill out dictionary
          dict_out[self.extr_str_processed] = str(1)
          bbox_str = get_bbox_str(one_det)
          # Encode the feature with base64
          dict_out[self.extr_str + "_" + bbox_str] = normfeatB64encode(one_feat)
    # Just featurize full image
    else:
      one_feat = self.featurizer.featurize(img_buffer)
      # force type to be as expected
      one_feat = one_feat.astype(get_feat_dtype(self.featurizer_type))
      dict_out[self.extr_str] = normfeatB64encode(one_feat)
      dict_out[self.extr_str_processed] = str(1)

    # Return dict ready to be pushed to DB
    return dict_out
