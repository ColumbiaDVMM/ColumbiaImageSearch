from __future__ import print_function
import gc
import sys
import time
import json
import math
import cStringIO
import threading
import traceback
from datetime import datetime
from argparse import ArgumentParser
# should column_list_sha1s be part of the list of columns retrieved from indexer?
#from cufacesearch.common import column_list_sha1s
from cufacesearch.common.error import full_trace_error
from cufacesearch.common.conf_reader import ConfReader
from cufacesearch.indexer.hbase_indexer_minimal import HBaseIndexerMinimal
  # , update_str_started, \
  # update_str_processed, update_str_completed, img_buffer_column, \
  # img_URL_column, img_path_column, EXTR_CF #, img_info_column_family, update_info_column_family
from cufacesearch.extractor.generic_extractor import DaemonBatchExtractor, GenericExtractor
from cufacesearch.extractor.generic_extractor import build_extr_str
from cufacesearch.ingester.generic_kafka_processor import GenericKafkaProcessor
from cufacesearch.imgio.imgio import buffer_to_B64

DEFAULT_EXTR_PROC_PREFIX = "EXTR_"
TIME_ELAPSED_FAILED = 3600
BATCH_SIZE_IMGBUFFER = 20
MAX_UP_CHECK_MISS_EXTR = 5

# Look for and process batch of a given extraction that have not been processed yet.
# Should be multi-threaded but single process...
def build_batch(list_in, batch_size):
  """Build a batch of ``batch_size`` samples from list ``list_in``

  :param list_in: input list
  :type list_in: list
  :param batch_size: batch size
  :type batch_size: int
  :yield: batch of size ``batch_size``
  """
  nbli = len(list_in)
  ibs = int(batch_size)
  if nbli > 0:
    for ndx in range(0, nbli, ibs):
      yield list_in[ndx:min(ndx + ibs, nbli)]
  else:
    yield []

class ThreadedDownloaderBufferOnly(threading.Thread):
  """ThreadedDownloaderBufferOnly class

  Multi-threaded download images from the web or load from disk.
  """

  def __init__(self, q_in, q_out, url_input=True):
    """ThreadedDownloaderBufferOnly constructor

    :param q_in: input queue
    :type q_in: :class:`Queue.Queue`
    :param q_out: output queue
    :type q_out: :class:`Queue.Queue`
    :param url_input: whether input are URLs (or files)
    :type url_input: bool
    """
    threading.Thread.__init__(self)
    self.q_in = q_in
    self.q_out = q_out
    self.url_input = url_input
    # If you have another way of getting the images based on SHA1
    # TODO: This could be a parameter... Should be passed down from ExtractionProcessor
    self.fallback_pattern = None
    # This is served by a DB, and we are hitting too heavily.
    # Wait to see if Tellfinder can dump the missing images.
    #self.fallback_pattern = "https://content.tellfinder.com/image/{}.jpeg"


  def run(self):
    """Perform download from web or loading from disk
    """
    from cufacesearch.imgio.imgio import get_buffer_from_URL, get_buffer_from_filepath

    while self.q_in.empty() is False:
      try:
        # The queue should already have items, no need to block
        (sha1, in_img, push_back) = self.q_in.get(False)
      except:
        continue

      try:
        if self.url_input:
          try:
            img_buffer = get_buffer_from_URL(in_img)
          except Exception as inst:
            # Transition: how could we get url from data:location?
            if self.fallback_pattern:
              # Adding fallback to Tellfinder images here
              # TODO: should we and how could we also update URL in DB?
              img_buffer = get_buffer_from_URL(self.fallback_pattern.format(sha1))
            else:
              raise inst
        else:
          img_buffer = get_buffer_from_filepath(in_img)
        if img_buffer:
          # Push
          self.q_out.put((sha1, buffer_to_B64(img_buffer), push_back, None))
      except Exception as inst:
        self.q_out.put((sha1, None, push_back, inst))

      # Mark as done
      self.q_in.task_done()

class ExtractionProcessor(ConfReader):
  """ExtractionProcessor class
  """

  def __init__(self, global_conf, prefix=DEFAULT_EXTR_PROC_PREFIX):
    """ExtractionProcessor constructor

    :param global_conf_in: configuration file or dictionary
    :type global_conf_in: str, dict
    :param prefix: prefix in configuration
    :type prefix: str
    """
    self.extractor = None
    self.nb_empt = 0
    self.nb_err = 0
    self.max_proc_time = 900 # in seconds. Increased for sbcmdline...
    self.url_input = True

    super(ExtractionProcessor, self).__init__(global_conf, prefix)

    # TODO: move that to self.read_conf()
    # Get required parameters
    self.input_type = self.get_required_param("input_type")
    self.nb_threads = self.get_required_param("nb_threads")
    self.featurizer_type = self.get_required_param("featurizer_type")
    self.featurizer_prefix = self.get_required_param("featurizer_prefix")
    self.detector_type = self.get_required_param("detector_type")

    # Get optional parameters
    self.verbose = int(self.get_param("verbose", default=0))
    self.maxucme = int(self.get_param("max_up_check_miss_extr", default=MAX_UP_CHECK_MISS_EXTR))
    self.ingestion_input = self.get_param("ingestion_input", default="kafka")
    self.push_back = self.get_param("push_back", default=False)
    file_input = self.get_param("file_input")
    print("[{}.ExtractionProcessor: log] file_input: {}".format(self.pp, file_input))
    if file_input:
      self.url_input = False
    print("[{}.ExtractionProcessor: log] url_input: {}".format(self.pp, self.url_input))

    # Means we extract feature from the whole image
    if self.detector_type == "full":
      self.detector = None

    self.extr_prefix = build_extr_str(self.featurizer_type, self.detector_type, self.input_type)
    self.set_pp()

    # Initialize queues
    self.init_queues()


    # Initialize indexer
    # We now have two indexers:
    # - one "in_indexer" for TF table with buffer, img URLs etc...
    # - one "out_indexer" for our table with extractions etc
    # NB: they could be the same if tables are merged...
    self.out_indexer = HBaseIndexerMinimal(self.global_conf,
                                           prefix=self.get_required_param("indexer_prefix"))
    self.out_indexer.pp = "ProcOutHBase"
    prefix_in_indexer = self.get_param("in_indexer_prefix", default=False)
    if prefix_in_indexer:
      self.in_indexer = HBaseIndexerMinimal(self.global_conf, prefix=prefix_in_indexer)
      self.in_indexer.pp = "ProcInHBase"
      insha1tab = self.in_indexer.table_sha1infos_name
      insha1cfs = self.in_indexer.get_dictcf_sha1_table()
      print("[{}] 'in_indexer' sha1 table {} columns are: {}".format(self.pp, insha1tab, insha1cfs))
    else:
      print("[{}] empty 'in_indexer_prefix', using out_indexer as in_indexer too.".format(self.pp))
      self.in_indexer = self.out_indexer
      self.in_indexer.pp = "ProcInOutHBase"

    # Initialize extractors only once (just one first)
    self.extractors = []
    # DONE: use 'out_indexer'
    self.extractors.append(GenericExtractor(self.detector_type, self.featurizer_type,
                                            self.input_type, self.out_indexer.extrcf,
                                            self.featurizer_prefix, self.global_conf))

    # DONE: use 'in_indexer'
    if self.url_input:
      self.img_column = self.in_indexer.get_col_imgurl()
    else:
      self.img_column = self.in_indexer.get_col_imgpath()
    img_cols = [self.in_indexer.get_col_imgbuff(), self.in_indexer.get_col_imgurlbak(),
                self.img_column]
    print("[{}.ExtractionProcessor: log] img_cols: {}".format(self.pp, img_cols))

    self.last_update_date_id = "1970-01-01"
    self.last_missing_extr_date = "1970-01-01"

    # Initialize ingester
    self.ingester = GenericKafkaProcessor(self.global_conf,
                                          prefix=self.get_required_param("proc_ingester_prefix"))
    self.ingester.pp = "ep"


  def set_pp(self, pp="ExtractionProcessor"):
    """Set pretty name

    :param pp: pretty name prefix
    :type pp: str
    """
    self.pp = pp
    if self.extractor:
      self.pp += "_"+self.extr_prefix

  def init_queues(self):
    """Initialize queues list ``self.q_in`` and ``self.q_out``
    """
    from multiprocessing import JoinableQueue
    self.q_in = []
    self.q_out = []
    for _ in range(self.nb_threads):
      self.q_in.append(JoinableQueue(0))
      self.q_out.append(JoinableQueue(0))


  # Should these two methods be in indexer?
  def is_update_unprocessed(self, update_id):
    """Check if an update was not processed yet

    :param update_id: update id
    :type update_id: str
    :return: boolean indicated if update ``update_id`` is unprocessed
    :rtype: bool
    """
    # DONE: use out_indexer
    update_rows = self.out_indexer.get_rows_by_batch([update_id],
                                                     table_name=self.out_indexer.table_updateinfos_name)
    if update_rows:
      for row in update_rows:
        if self.out_indexer.get_col_upproc() in row[1]:
          return False
    return True

  def is_update_notstarted(self, update_id, max_delay=None):
    """Check if an update was not started yet

    :param update_id: update id
    :type update_id: str
    :param max_delay: delay (in seconds) between marked start time and now to consider update failed
    :type max_delay: int
    :return: boolean
    :rtype: bool
    """
    # DONE: use out_indexer
    update_rows = self.out_indexer.get_rows_by_batch([update_id],
                                                     table_name=self.out_indexer.table_updateinfos_name)
    if update_rows:
      for row in update_rows:
        # changed to: self.column_update_started
        #if info_column_family+":"+update_str_started in row[1]:
        #if self.column_update_started in row[1]:
        # DONE: use out_indexer
        if self.out_indexer.get_col_upstart() in row[1]:
          if max_delay:
            start_str = row[1][self.out_indexer.get_col_upstart()]
            # start time format is '%Y-%m-%d:%H.%M.%S'
            start_dt = datetime.strptime(start_str, '%Y-%m-%d:%H.%M.%S')
            now_dt = datetime.now()
            diff_dt = now_dt - start_dt
            if diff_dt.total_seconds() > max_delay:
              return True
          return False
    return True

  def get_batch_hbase(self):
    """Get one batch of images from HBase

    :yield: tuple (rows_batch, update_id)
    """
    # legacy implementation: better to have a kafka topic for batches to be processed to allow
    # safe and efficient parallelization on different machines
    # DONE: use in_indexer
    img_cols = [self.in_indexer.get_col_imgbuff(), self.in_indexer.get_col_imgurlbak(),
                self.img_column]
    try:
      # DONE: use out_indexer
      for updates in self.out_indexer.get_unprocessed_updates_from_date(self.last_update_date_id,
                                                                        extr_type=self.extr_prefix):
        for update_id, update_cols in updates:
          if self.extr_prefix in update_id:
            # double check update has not been processed somewhere else
            if self.is_update_unprocessed(update_id):
              # double check update was not marked as started recently i.e. by another process
              if self.is_update_notstarted(update_id, max_delay=TIME_ELAPSED_FAILED):
                # DONE: use out_indexer
                list_sha1s = update_cols[self.out_indexer.get_col_listsha1s()].split(',')
                msg = "[{}.get_batch_hbase: log] Update {} has {} images."
                print(msg.format(self.pp, update_id, len(list_sha1s)))
                # We should time that, it seems slow i.e. 2/3 minutes per update.
                try:
                  rows_batch = self.in_indexer.get_columns_from_sha1_rows(list_sha1s,
                                                                          rbs=BATCH_SIZE_IMGBUFFER,
                                                                          columns=img_cols)
                except Exception:
                  msg = "[{}.get_batch_hbase: warning] Failed retrieving images data for update: {}"
                  print(msg.format(self.pp, update_id))
                  # flush?
                  sys.stdout.flush()
                  # Update self.last_update_date_id ?
                  #self.last_update_date_id = '_'.join(update_id.split('_')[-2:])
                  continue
                # print "rows_batch", rows_batch
                if rows_batch:
                  yield rows_batch, update_id
                  self.last_update_date_id = '_'.join(update_id.split('_')[-2:])
                else:
                  msg = "[{}.get_batch_hbase: log] Did not get any image buffer for update: {}"
                  print(msg.format(self.pp, update_id))
                  #msg = "[{}.get_batch_hbase: log] Was trying to read columns {} from table {} for rows {}"
                  #print(msg.format(self.pp, img_cols, self.in_indexer.table_sha1infos_name, list_sha1s))
              else:
                msg = "[{}.get_batch_hbase: log] Skipping update started recently: {}"
                print(msg.format(self.pp, update_id))
                continue
            else:
              msg = "[{}.get_batch_hbase: log] Skipping already processed update: {}"
              print(msg.format(self.pp, update_id))
              continue
          else:
            if self.verbose > 6:
              msg = "[{}.get_batch_hbase: log] Skipping update {} from another extraction type."
              print(msg.format(self.pp, update_id))
      else:
        print("[{}.get_batch_hbase: log] No unprocessed update found.".format(self.pp))
        # Should we reinitialized self.last_update_date_id?
        # Look for updates that have some unprocessed images
        # TODO: wether we do that or not could be specified by a parameter
        # as this induces slow down during update...
        # DONE: use out_indexer
        count_ucme = 0
        stop_cme = False
        for updates in self.out_indexer.get_missing_extr_updates_from_date(self.last_missing_extr_date,
                                                                           extr_type=self.extr_prefix):
          for update_id, update_cols in updates:
            if self.extr_prefix in update_id:
              # DONE: use out_indexer
              if self.out_indexer.get_col_listsha1s() in update_cols:
                list_sha1s = update_cols[self.out_indexer.get_col_listsha1s()].split(',')
                msg = "[{}.get_batch_hbase: log] Update {} has {} images missing extractions."
                print(msg.format(self.pp, update_id, len(list_sha1s)))
                sys.stdout.flush()
                # also get 'ext:' to check if extraction was already processed?
                # DONE: use in_indexer
                rows_batch = self.in_indexer.get_columns_from_sha1_rows(list_sha1s,
                                                                        rbs=BATCH_SIZE_IMGBUFFER,
                                                                        columns=img_cols)
                if rows_batch:
                  yield rows_batch, update_id
                  self.last_missing_extr_date = '_'.join(update_id.split('_')[-2:])
                  count_ucme +=1
                  if count_ucme >= self.maxucme:
                    stop_cme = True
                    break
                else:
                  msg = "[{}.get_batch_hbase: log] Did not get any image buffer for update: {}"
                  print(msg.format(self.pp, update_id))
              else:
                msg = "[{}.get_batch_hbase: log] Update {} has no images list."
                print(msg.format(self.pp, update_id))
            else:
              msg = "[{}.get_batch_hbase: log] Skipping update {} from another extraction type."
              print(msg.format(self.pp, update_id))
          # We have reached maximum number of check for missing extractions in one call
          if stop_cme:
            break
        else:
          if stop_cme:
            msg = "[{}.get_batch_hbase: log] Stopped checking updates with missing extractions"
            msg += "after founding {}/{}."
            print(msg.format(self.pp, count_ucme, self.maxucme, self.last_missing_extr_date))
            msg = "[{}.get_batch_hbase: log] Will restart next time from: {}"
            print(msg.format(self.pp, self.last_missing_extr_date))
            sys.stdout.flush()
          else:
            msg = "[{}.get_batch_hbase: log] No updates with missing extractions found."
            print(msg.format(self.pp))
            sys.stdout.flush()
            # Re-initialize dates just to make sure we don't miss anything
            self.last_update_date_id = "1970-01-01"
            self.last_missing_extr_date = "1970-01-01"

    except Exception as inst:
      # If we reach this point it is really a succession of failures
      full_trace_error("[{}.get_batch_hbase: error] {}".format(self.pp, inst))
      # Raise Exception to restart process or docker
      raise inst



  def get_batch_kafka(self):
    """Get one batch of images from Kafka

    :yield: tuple (rows_batch, update_id)
    """
    # Read from a kafka topic to allow safer parallelization on different machines
    # DONE: use in_indexer
    img_cols = [self.in_indexer.get_col_imgbuff(), self.in_indexer.get_col_imgurlbak(),
                self.img_column]
    try:
      # Needs to read topic to get update_id and list of sha1s
      if self.ingester.consumer:
        for msg in self.ingester.consumer:
          msg_dict = json.loads(msg.value)
          update_id = msg_dict.keys()[0]
          # NB: Try to get update info and check it was really not processed yet.
          if self.is_update_unprocessed(update_id):
            str_list_sha1s = msg_dict[update_id]
            list_sha1s = str_list_sha1s.split(',')
            msg = "[{}.get_batch_kafka: log] Update {} has {} images."
            print(msg.format(self.pp, update_id, len(list_sha1s)))
            if self.verbose > 3:
              msg = "[{}.get_batch_kafka: log] Looking for columns: {}"
              print(msg.format(self.pp, img_cols))
            # DONE: use in_indexer
            #rows_batch = self.in_indexer.get_columns_from_sha1_rows(list_sha1s, columns=img_cols)
            rows_batch = self.in_indexer.get_columns_from_sha1_rows(list_sha1s,
                                                                    rbs=BATCH_SIZE_IMGBUFFER,
                                                                    columns=img_cols)
            #print "rows_batch", rows_batch
            if rows_batch:
              if self.verbose > 4:
                msg = "[{}.get_batch_kafka: log] Yielding for update: {}"
                print(msg.format(self.pp, update_id))
              yield rows_batch, update_id
              self.ingester.consumer.commit()
              if self.verbose > 4:
                msg = "[{}.get_batch_kafka: log] After yielding for update: {}"
                print(msg.format(self.pp, update_id))
              self.last_update_date_id = '_'.join(update_id.split('_')[-2:])
            # Should we try to commit offset only at this point?
            else:
              msg = "[{}.get_batch_kafka: log] Did not get any image buffers for the update: {}"
              print(msg.format(self.pp, update_id))
          else:
            msg = "[{}.get_batch_kafka: log] Skipping already processed update: {}"
            print(msg.format(self.pp, update_id))
        else:
          print("[{}.get_batch_kafka: log] No update found.".format(self.pp))
          # Fall back to checking HBase for unstarted/unfinished updates
          for rows_batch, update_id in self.get_batch_hbase():
            yield rows_batch, update_id
      else:
        print("[{}.get_batch_kafka: log] No consumer found.".format(self.pp))
        # Fall back to checking HBase for unstarted/unfinished updates
        for rows_batch, update_id in self.get_batch_hbase():
          yield rows_batch, update_id
    except Exception as inst:
      # If we reach this point it is really a succession of failures
      full_trace_error("[{}.get_batch_kafka: error] {}".format(self.pp, inst))
      # Raise Exception to restart process or docker
      raise inst



  def get_batch(self):
    """Get one batch of images

    :yield: tuple (rows_batch, update_id)
    """
    if self.ingestion_input == "hbase":
      for rows_batch, update_id in self.get_batch_hbase():
        yield rows_batch, update_id
    else:
      for rows_batch, update_id in self.get_batch_kafka():
        yield rows_batch, update_id

  def process_batch(self):
    """Process one batch of images

    :raises Exception: if something goes really wrong
    """
    # Get a new update batch
    try:
      for rows_batch, update_id in self.get_batch():
        start_update = time.time()
        print("[{}] Processing update {} of {} rows.".format(self.pp, update_id, len(rows_batch)))
        sys.stdout.flush()

        # Initialize
        self.nb_empt = 0
        self.init_queues()
        threads = []

        # If we have deleted an extractor at some point or for first batch
        nb_extr_to_create = self.nb_threads - len(self.extractors)
        if nb_extr_to_create:
          start_create_extractor = time.time()
          while len(self.extractors) < min(self.nb_threads, len(rows_batch)):
            # DONE: use 'out_indexer'
            self.extractors.append(GenericExtractor(self.detector_type, self.featurizer_type,
                                                    self.input_type, self.out_indexer.extrcf,
                                                    self.featurizer_prefix, self.global_conf))
          msg = "[{}] Created {} extractors in {}s."
          create_extr_time = time.time() - start_create_extractor
          print(msg.format(self.pp, len(self.extractors), create_extr_time))


        # Mark batch as started to be process
        now_str = datetime.now().strftime('%Y-%m-%d:%H.%M.%S')
        # changed to: self.column_update_started
        #dict_val = {info_column_family + ':' + update_str_started: now_str}
        #dict_val = {self.column_update_started: now_str}
        # DONE: use out_indexer
        dict_val = {self.out_indexer.get_col_upstart(): now_str}
        update_started_dict = {update_id: dict_val}
        # DONE: use out_indexer
        self.out_indexer.push_dict_rows(dict_rows=update_started_dict,
                                        table_name=self.out_indexer.table_updateinfos_name)

        # TODO: define a get_buffer_images method
        # --------
        # Push images to queue
        list_in = []
        # For parallelized downloading...
        from Queue import Queue
        nb_imgs_dl = 0
        q_in_dl = Queue(0)
        q_out_dl = Queue(0)

        start_get_buffer = time.time()
        # DONE: use in_indexer in all this scope
        # How could we transfer URL from in table to out table if they are different?...
        for img in rows_batch:
          # should decode base64
          #if img_buffer_column in img[1]:
          if self.in_indexer.get_col_imgbuff() in img[1]:
            # That's messy...
            # b64buffer = buffer_to_B64(cStringIO.StringIO(img[1][self.in_indexer.get_col_imgbuff()]))
            # use img[1].pop(self.in_indexer.get_col_imgbuff())
            b64buffer = buffer_to_B64(cStringIO.StringIO(img[1].pop(self.in_indexer.get_col_imgbuff())))
            tup = (img[0], b64buffer, False)
            list_in.append(tup)
          else:
            # need to re-download, accumulate a list of URLs to download
            # Deal with img_path_column for local_images_kafka_pusher
            if self.img_column in img[1]:
              q_in_dl.put((img[0], img[1][self.img_column], self.push_back))
              nb_imgs_dl += 1
            elif self.in_indexer.get_col_imgurlbak() in img[1]:
              q_in_dl.put((img[0], img[1][self.in_indexer.get_col_imgurlbak()], self.push_back))
              nb_imgs_dl += 1
            else:
              msg = "[{}: warning] No buffer and no URL/path for image {} !"
              print(msg.format(self.pp, img[0]))
              continue

        # Download missing images
        nb_dl = 0
        nb_dl_failed = 0
        if nb_imgs_dl > 0:
          threads_dl = []
          for i in range(min(self.nb_threads, nb_imgs_dl)):
            # should read (url, obj_pos) from self.q_in
            # and push (url, obj_pos, buffer, img_info, start_process, end_process) to self.q_out
            thread = ThreadedDownloaderBufferOnly(q_in_dl, q_out_dl, url_input=self.url_input)
            thread.start()
            threads_dl.append(thread)

          q_in_dl.join()

          # Push downloaded images to list_in too
          while nb_dl < nb_imgs_dl:
            # This can block?
            #sha1, buffer, push_back, inst = q_out_dl.get()
            try:
              sha1, buffer, push_back, inst = q_out_dl.get(True, 10)
            except Exception as queue_err:
              msg = "[{}: error] Download queue out timed out: {}"
              print(msg.format(self.pp, queue_err))
              break
            nb_dl += 1
            if inst:
              if self.verbose > 6:
                msg = "[{}: log] Could not download image {}, error was: {}"
                print(msg.format(self.pp, sha1, inst))
              nb_dl_failed += 1
            else:
              if buffer:
                list_in.append((sha1, buffer, push_back))
              else:
                # Is that even possible?
                msg = "[{}: error] No error but no buffer either for image {}"
                print(msg.format(self.pp, sha1))

        get_buffer_time = time.time() - start_get_buffer
        msg = "[{}] Got {}/{} image buffers ({}/{} downloaded) for update {} in {}s."
        print(msg.format(self.pp, len(list_in), len(rows_batch), nb_dl - nb_dl_failed, nb_dl,
                         update_id, get_buffer_time))
        sys.stdout.flush()

        # --------
        # if len(list_in) == 0, we shouldn't try to process anything, just mark update as processed
        if len(list_in) != 0:

          # TODO: define a get_features method
          # --------
          q_batch_size = int(math.ceil(float(len(list_in))/self.nb_threads))
          for i, q_batch in enumerate(build_batch(list_in, q_batch_size)):
            self.q_in[i].put(q_batch)

          q_in_size = []
          q_in_size_tot = 0
          for i in range(self.nb_threads):
            q_in_size.append(self.q_in[i].qsize())
            q_in_size_tot += q_in_size[i]
          if self.verbose > 5:
            print("[{}] Total input queues sizes is: {}".format(self.pp, q_in_size_tot))

          # Start daemons...
          thread_creation_failed = [0] * self.nb_threads
          for i in range(self.nb_threads):
            # one per non empty input queue
            if q_in_size[i] > 0:
              try:
                thread = DaemonBatchExtractor(self.extractors[i], self.q_in[i], self.q_out[i],
                                              verbose=self.verbose)
                # Could get a 'Cannot allocate memory' if we are using too many threads...
                thread.start()
                threads.append(thread)
              except OSError as inst:
                # Should we try to push self.q_in[i] data to some other thread?
                msg = "[{}.process_batch: error] Could not start thread #{}: {}"
                print(msg.format(self.pp, i+1, inst))
                thread_creation_failed[i] = 1
                time.sleep(sum(thread_creation_failed))

          if sum(thread_creation_failed) == self.nb_threads:
            # We are in trouble...
            raise RuntimeError("Could not start any thread...")

          nb_threads_running = len(threads)
          deleted_extr = [0] * nb_threads_running
          start_process = time.time()
          stop = time.time() + self.max_proc_time
          # Wait for all tasks to be marked as done
          threads_finished = [0] * nb_threads_running
          thread_msg = "[{}] Thread {}/{} (pid: {}) "
          while sum(threads_finished) < nb_threads_running:
            for i in range(nb_threads_running):
              if sum(threads_finished) == nb_threads_running:
                sys.stdout.flush()
                break
              if threads_finished[i] == 1:
                continue
              i_q_in = i + sum(thread_creation_failed[:i + 1])
              if q_in_size[i_q_in] > 0:
                # This seems to block forever sometimes, if subprocess crashed?...
                #self.q_in[i].join()
                # Manual join with timeout...
                # https://github.com/python/cpython/blob/3.6/Lib/multiprocessing/queues.py
                if not self.q_in[i_q_in]._unfinished_tasks._semlock._is_zero() and time.time() < stop:
                  time.sleep(1)
                else:
                  if self.q_in[i_q_in]._unfinished_tasks._semlock._is_zero():
                    if self.verbose > 5:
                      msg = thread_msg+"marked as finished because processing seems finished"
                      print(msg.format(self.pp, i+1, nb_threads_running, threads[i].pid))
                  else:
                    if self.verbose > 0:
                      # In this cases does this happen...
                      msg = thread_msg+"force marked task as done as max_proc_time ({}) has passed."
                      print(msg.format(self.pp, i+1, nb_threads_running, threads[i].pid,
                                       self.max_proc_time))
                      sys.stdout.flush()
                      # Try to delete corresponding extractor to free memory?
                      # And reduce number of threads at the end of the loop
                    try:
                      self.q_in[i_q_in].task_done()
                      if deleted_extr[i] == 0:
                        # we pushed the extractor as self.extractors[i] in a loop of self.nb_threads
                        # we use i_q_in
                        #del self.extractors[i_q_in]
                        del self.extractors[i_q_in - sum(deleted_extr[:i])]
                        deleted_extr[i] = 1
                    except Exception:
                      pass
                  threads_finished[i] = 1
              else:
                if self.verbose > 2:
                  # We actually never gave something to process...
                  msg = thread_msg+"marked as finished because no data was passed to it"
                  print(msg.format(self.pp, i+1, nb_threads_running, threads[i].pid))
                threads_finished[i] = 1

          # Cleanup threads to free memory before getting data back
          # Daemon may still be running...
          # and will actually be deleted only when they exit after not getting a batch
          del threads

          # Gather results
          q_out_size = []
          q_out_size_tot = 0
          for i in range(self.nb_threads):
            q_out_size.append(self.q_out[i].qsize())
            q_out_size_tot += q_out_size[i]

          if self.verbose > 5:
            print("[{}: log] Total output queues size is: {}".format(self.pp, q_out_size_tot))
            sys.stdout.flush()

          # Can get stuck here?
          dict_imgs = dict()
          if q_out_size_tot > 0:
            for i in range(self.nb_threads):
              if self.verbose > 4:
                print("[{}] Thread {} q_out_size: {}".format(self.pp, i + 1, q_out_size[i]))
                sys.stdout.flush()
              while q_out_size[i] > 0 and not self.q_out[i].empty():
                if self.verbose > 6:
                  print("[{}] Thread {} q_out is not empty.".format(self.pp, i + 1))
                  sys.stdout.flush()
                try:
                  # This can still block forever?
                  batch_out = self.q_out[i].get(True, 10)
                  if self.verbose > 4:
                    msg = "[{}] Got batch of {} features from thread {} q_out."
                    print(msg.format(self.pp, len(batch_out), i + 1))
                    sys.stdout.flush()
                  for sha1, dict_out in batch_out:
                    dict_imgs[sha1] = dict_out
                except:
                  if self.verbose > 1:
                    print("[{}] Thread {} failed to get from q_out: {}".format(self.pp, i + 1))
                    sys.stdout.flush()
                  #pass
                if self.verbose > 4:
                  print("[{}] Marking task done in q_out of thread {}.".format(self.pp, i + 1))
                  sys.stdout.flush()
                self.q_out[i].task_done()

          #if self.verbose > 0:
          print_msg = "[{}] Got features for {}/{} images in {}s."
          proc_time = time.time() - start_process
          print(print_msg.format(self.pp, len(dict_imgs.keys()), len(list_in), proc_time))
          sys.stdout.flush()
          # --------

          if len(dict_imgs.keys()) > 0:
            # Push computed features
            self.out_indexer.push_dict_rows(dict_rows=dict_imgs,
                                            table_name=self.out_indexer.table_sha1infos_name)

        else:
          msg = "[{}: Warning] Could not get any image buffer (out of {} requested) for update {}"
          print(msg.format(self.pp, len(rows_batch), update_id))
          dict_imgs = dict()
          nb_threads_running = len(threads)
          thread_creation_failed = [0] * self.nb_threads
          deleted_extr = [0] * nb_threads_running

        # Mark batch as processed
        now_str = datetime.now().strftime('%Y-%m-%d:%H.%M.%S')
        # DONE: use out_indexer
        update_processed_dict = {update_id: {self.out_indexer.get_col_upproc(): now_str}}
        self.out_indexer.push_dict_rows(dict_rows=update_processed_dict,
                                        table_name=self.out_indexer.table_updateinfos_name)

        # Mark as completed if all rows had an extraction
        if len(rows_batch) == len(dict_imgs.keys()):
          # DONE: use out_indexer
          update_completed_dict = {update_id: {self.out_indexer.get_col_upcomp(): str(1)}}
          self.out_indexer.push_dict_rows(dict_rows=update_completed_dict,
                                          table_name=self.out_indexer.table_updateinfos_name)

        # Cleanup
        del self.q_in
        del self.q_out

        # To try to adjust a too optimistic nb_threads setting
        # if (sum(thread_creation_failed) > 0 or sum(deleted_extr) > 0) and self.nb_threads > 2:
        #   self.nb_threads -= 1

        msg = "[{}] Completed update {} in {}s."
        print(msg.format(self.pp, update_id, time.time() - start_update))
        sys.stdout.flush()
        self.nb_err = 0

        # Force garbage collection
        gc.collect()

        # Should we just raise an Exception and restart clean?
        if sum(thread_creation_failed) > 0 or sum(deleted_extr) > 0:
          # To try to adjust a too optimistic nb_threads setting
          if self.nb_threads > 1:
            self.nb_threads -= 1
            self.extractors = []
            gc.collect()
          else:
            raise RuntimeError("Processed failed with a single thread...")

    except Exception as inst:
      #exc_type, exc_obj, exc_tb = sys.exc_info()
      #fulltb = traceback.format_tb(exc_tb)
      print("[{}] {}".format(self.pp, inst))
      #print("[{}] {} ({})".format(self.pp, inst, ''.join(fulltb)))
      # Things are likely to be very bad at that point... Docker should be restarted
      #if self.nb_threads == 2:
      raise inst
      #raise type(inst)(" {} ({})".format(inst, ''.join(fulltb)))

  def run(self):
    """Run processor
    """
    self.nb_empt = 0
    self.nb_err = 0
    while True:
      self.process_batch()
      msg = "[ExtractionProcessor: log] Nothing to process at: {}"
      print(msg.format(datetime.now().strftime('%Y-%m-%d:%H.%M.%S')))
      sys.stdout.flush()
      time.sleep(10*min(self.nb_empt, 60))
      self.nb_empt += 1

      # try:
      #   self.process_batch()
      #   print("[ExtractionProcessor: log] Nothing to process at: {}".format(datetime.now().strftime('%Y-%m-%d:%H.%M.%S')))
      #   sys.stdout.flush()
      #   time.sleep(10*self.nb_empt)
      #   self.nb_empt += 1
      # except Exception as inst:
      #   err_msg = "ExtractionProcessor died: {} {}".format(type(inst), inst)
      #   full_trace_error(err_msg)
      #   sys.stdout.flush()
      #   time.sleep(10 * self.nb_err)
      #   self.nb_err += 1


if __name__ == "__main__":

  # Get conf file
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  parser.add_argument("-p", "--prefix", dest="prefix", default=DEFAULT_EXTR_PROC_PREFIX)
  options = parser.parse_args()

  # Initialize extraction processor
  ep = ExtractionProcessor(options.conf_file, prefix=options.prefix)

  print("Extraction processor options are: {}".format(options))
  sys.stdout.flush()

  ep.run()

  # nb_err = 0
  # while True:
  #   try:
  #     ep.run()
  #     nb_err = 0
  #   except Exception as inst:
  #     full_trace_error("Extraction processor failed: {}".format(inst))
  #     sys.stdout.flush()
  #     break
  #     #raise inst
  #     # del ep
  #     # gc.collect()
  #     # time.sleep(10*nb_err)
  #     # ep = ExtractionProcessor(options.conf_file, prefix=options.prefix)
  #     # nb_err += 1