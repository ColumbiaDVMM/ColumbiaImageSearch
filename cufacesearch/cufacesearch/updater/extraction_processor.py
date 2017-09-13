from __future__ import print_function
import sys
import time
import math
from datetime import datetime
from argparse import ArgumentParser
from cufacesearch.common import column_list_sha1s
from cufacesearch.common.error import full_trace_error
from cufacesearch.common.conf_reader import ConfReader
from cufacesearch.indexer.hbase_indexer_minimal import HBaseIndexerMinimal, update_str_started, update_str_processed
from cufacesearch.extractor.generic_extractor import DaemonBatchExtractor, GenericExtractor, build_extr_str

default_extr_proc_prefix = "EXTR_"

# Look for and process batch of a given extraction that have not been processed yet.
# Should be multi-threaded but single process...
def build_batch(list_in, batch_size):
  l = len(list_in)
  ibs = int(batch_size)
  for ndx in range(0, l, ibs):
    yield list_in[ndx:min(ndx + ibs, l)]

class ExtractionProcessor(ConfReader):

  def __init__(self, global_conf, prefix=default_extr_proc_prefix):
    self.extractor = None

    super(ExtractionProcessor, self).__init__(global_conf, prefix)

    self.featurizer_type = self.get_required_param("featurizer_type")
    self.featurizer_prefix = self.get_required_param("featurizer_prefix")
    self.detector_type = self.get_required_param("detector_type")
    if self.detector_type == "full":
      self.detector = None
    self.input_type = self.get_required_param("input_type")
    self.nb_threads = self.get_required_param("nb_threads")

    self.verbose = 0
    verbose = self.get_param("verbose")
    if verbose:
      self.verbose = int(verbose)

    # Need to be build from extraction type and detection input + "_processed"
    self.extr_family_column = "ext"
    tmp_extr_family_column = self.get_param("extr_family_column")
    if tmp_extr_family_column:
      self.extr_family_column = tmp_extr_family_column

    self.extr_prefix = build_extr_str(self.featurizer_type, self.detector_type, self.input_type)

    # Initialize queues
    from multiprocessing import JoinableQueue as Queue
    self.q_in = []
    self.q_out = []
    for i in range(self.nb_threads):
      self.q_in.append(Queue(0))
      self.q_out.append(Queue(0))

    self.extractors = []
    for i in range(self.nb_threads):
      self.extractors.append(GenericExtractor(self.detector_type, self.featurizer_type, self.input_type,
                                      self.extr_family_column, self.featurizer_prefix, self.global_conf))

    # Beware, the self.extr_family_column should be added to the indexer families parameter in get_create_table...
    self.tablesha1_col_families = {'info': dict(), self.extr_family_column: dict()}

    self.set_pp()

    # Initialize indexer
    self.indexer = HBaseIndexerMinimal(self.global_conf, prefix=self.get_required_param("indexer_prefix"))
    self.last_update_date_id = ''



  def set_pp(self):
    self.pp = "ExtractionProcessor"
    if self.extractor:
      self.pp += "_"+self.extr_prefix

  def get_batch(self):
    try:
      # needs to read update table rows starting with 'index_update_'+extr and not marked as indexed.
      list_updates = self.indexer.get_unprocessed_updates_from_date(self.last_update_date_id, extr_type="_"+self.extr_prefix)
      if list_updates:
        for update_id, update_cols in list_updates:
          str_list_sha1s = update_cols[column_list_sha1s]
          list_sha1s = str_list_sha1s.split(',')
          print ("[{}.get_batch: log] Update {} has {} images.".format(self.pp, update_id, len(list_sha1s)))
          # also get 'ext:' to check if extraction was already processed?
          rows_batch = self.indexer.get_columns_from_sha1_rows(list_sha1s, columns=["info:img_buffer"])
          #print "rows_batch", rows_batch
          if rows_batch:
            print("[{}.get_batch: log] Yielding for update: {}".format(self.pp, update_id))
            yield rows_batch, update_id
            print("[{}.get_batch: log] After yielding for update: {}".format(self.pp, update_id))
            self.last_update_date_id = '_'.join(update_id.split('_')[-2:])
          else:
            print("[{}.get_batch: log] Did not get any image buffers for the update: {}".format(self.pp, update_id))
      else:
        print("[{}.get_batch: log] Nothing to update!".format(self.pp))
    except Exception as inst:
      full_trace_error("[{}.get_batch: error] {}".format(self.pp, inst))

  def process_batch(self):
    # Get a new batch from update table
    for rows_batch, update_id in self.get_batch():
      start_update = time.time()
      print("[{}.process_batch: log] Processing update: {}".format(self.pp, update_id))
      sys.stdout.flush()

      threads = []

      # Mark batch as started to be process
      update_started_dict = {update_id: {'info:' + update_str_started: datetime.now().strftime('%Y-%m-%d:%H.%M.%S')}}
      self.indexer.push_dict_rows(dict_rows=update_started_dict, table_name=self.indexer.table_updateinfos_name)

      # Push images to queue

      list_in = []
      for img in rows_batch:
        # should decode base64
        tup = (img[0], img[1]["info:img_buffer"])
        list_in.append(tup)
      q_batch_size = int(math.ceil(float(len(list_in))/self.nb_threads))
      for i, q_batch in enumerate(build_batch(list_in, q_batch_size)):
        self.q_in[i].put(q_batch)

      q_in_size = []
      q_in_size_tot = 0
      for i in range(self.nb_threads):
        q_in_size.append(self.q_in[i].qsize())
        q_in_size_tot += q_in_size[i]
      if self.verbose > 1:
        print("[{}.process_batch: log] Total input queues sizes is: {}".format(self.pp, q_in_size_tot))

      # Start daemons...
      for i in range(self.nb_threads):
        # one per non empty input queue
        if q_in_size[i] > 0:
          thread = DaemonBatchExtractor(self.extractors[i], self.q_in[i], self.q_out[i], verbose=self.verbose)
          thread.start()
          threads.append(thread)

      start_process = time.time()
      # Wait for all tasks to be marked as done
      for i in range(self.nb_threads):
        if q_in_size[i] > 0:
          self.q_in[i].join()

      # Gather results
      q_out_size = []
      q_out_size_tot = 0
      for i in range(self.nb_threads):
        q_out_size.append(self.q_out[i].qsize())
        q_out_size_tot += q_out_size[i]

      if self.verbose > 1:
        print("[{}.process_batch: log] Total output queues size is: {}".format(self.pp, q_out_size_tot))

      dict_imgs = dict()
      for i in range(self.nb_threads):
        while not self.q_out[i].empty():
          batch_out = self.q_out[i].get()
          for sha1, dict_out in batch_out:
            dict_imgs[sha1] = dict_out
          self.q_out[i].task_done()

      if self.verbose > 1:
        print_msg = "[{}.process_batch: log] Got features for {} images in {}s."
        print(print_msg.format(self.pp, len(dict_imgs.keys()), time.time() - start_process))
        sys.stdout.flush()

      # Push them
      self.indexer.push_dict_rows(dict_rows=dict_imgs, table_name=self.indexer.table_sha1infos_name)

      # Mark batch as processed
      update_processed_dict = {update_id: {'info:' + update_str_processed: datetime.now().strftime('%Y-%m-%d:%H.%M.%S')}}
      self.indexer.push_dict_rows(dict_rows=update_processed_dict, table_name=self.indexer.table_updateinfos_name)

      # Cleanup
      for th in threads:
        del th

      print_msg = "[{}.process_batch: log] Completed update {} in {}s."
      print(print_msg.format(self.pp, update_id, time.time() - start_update))
      sys.stdout.flush()

  def run(self):
    nb_empt = 0
    while True:
      self.process_batch()
      print("[ExtractionProcessor: log] Nothing to process at: {}".format(datetime.now().strftime('%Y-%m-%d:%H.%M.%S')))
      sys.stdout.flush()
      time.sleep(10*nb_empt)
      nb_empt += 1


if __name__ == "__main__":

  # Get conf file
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  parser.add_argument("-p", "--prefix", dest="prefix", default=default_extr_proc_prefix)
  options = parser.parse_args()

  # Initialize extraction processor
  ep = ExtractionProcessor(options.conf_file, prefix=options.prefix)
  nb_err = 0

  print("Starting extraction {}".format(ep.extr_prefix))
  while True:
    try:
      ep.run()
    except Exception as inst:
      full_trace_error("Extraction processor failed: {}".format(inst))
      time.sleep(10*nb_err)
      nb_err += 1