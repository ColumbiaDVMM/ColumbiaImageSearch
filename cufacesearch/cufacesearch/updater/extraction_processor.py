from copy import deepcopy
import time
from datetime import datetime
from argparse import ArgumentParser
from cufacesearch.common import column_list_sha1s
from cufacesearch.common.error import full_trace_error
from cufacesearch.common.conf_reader import ConfReader
from cufacesearch.imgio.imgio import get_buffer_from_B64
from cufacesearch.indexer.hbase_indexer_minimal import HBaseIndexerMinimal, update_str_started, update_str_processed
from cufacesearch.extractor.generic_extractor import ThreadedExtractor, GenericExtractor, build_extr_str

default_extr_proc_prefix = "EXTR_"

# Look for and process batch of a given extraction that have not been processed yet.
# Should be multi-threaded but single process...

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

    # Need to be build from extraction type and detection input + "_processed"
    self.extr_family_column = "ext"
    tmp_extr_family_column = self.get_param("extr_family_column")
    if tmp_extr_family_column:
      self.extr_family_column = tmp_extr_family_column

    self.extr_prefix = build_extr_str(self.featurizer_type, self.detector_type, self.input_type)

    # self.extractor = GenericExtractor(self.detector_type, self.featurizer_type, self.input_type,
    #                                         self.extr_family_column, self.featurizer_prefix, self.global_conf)

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

    # Initialize queues
    from Queue import Queue
    self.q_in = Queue(0)
    self.q_out = Queue(0)

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
          print("[{}.get_batch: log] Update {} has {} images.".format(self.pp, update_id, len(list_sha1s)))
          # also get 'ext:' to check if extraction was already processed?
          rows_batch = self.indexer.get_columns_from_sha1_rows(list_sha1s, columns=["info:img_buffer"])
          #print "rows_batch", rows_batch
          if rows_batch:
            print("[{}.get_batch: log] Yielding for update: {}".format(self.pp, update_id))
            yield rows_batch, update_id
            print("[{}.get_batch: log] After yielding for update: {}".format(self.pp, update_id))
            self.last_update_date_id = '_'.join(update_id.split('_')[-2:])
          else:
            print("[{}.get_batch: log] Did not get any image buffers for the update: {}".format(update_id))
      else:
        print "[{}.get_batch: log] Nothing to update!".format(self.pp)
    except Exception as inst:
      full_trace_error("[{}.get_batch: error] {}".format(self.pp, inst))

  def process_batch(self):
    # Get a new batch from update table
    for rows_batch, update_id in self.get_batch():
      print("[{}.process_batch: log] Processing update: {}".format(self.pp, update_id))
      threads = []

      # Mark batch as started to be process
      update_started_dict = {'update_id': {'info:' + update_str_started: datetime.now().strftime('%Y-%m-%d:%H.%M.%S')}}
      self.indexer.push_dict_rows(dict_rows=update_started_dict, table_name=self.indexer.table_updateinfos_name)

      # Push images to queue
      for img in rows_batch:
        # should decode base64
        tup = (img[0], get_buffer_from_B64(img[1]["info:img_buffer"]))
        self.q_in.put(tup)

      # Start threads, at most one per image
      for i in range(min(self.nb_threads, len(rows_batch))):
        # should read (sha1, img_buffer) from self.q_in
        # and push (sha1, dict_out) to self.q_out
        # RuntimeError: Pickling of "caffe._caffe.Net" instances is not enabled (http://www.boost.org/libs/python/doc/v2/pickle.html)
        #thread = ThreadedExtractor(deepcopy(self.extractor), self.q_in, self.q_out)
        thread = ThreadedExtractor(self.extractors[i], self.q_in, self.q_out)
        thread.start()
        threads.append(thread)

      # Wait for all tasks to be marked as done
      if self.q_in.qsize() > 0:
        self.q_in.join()

      # Gather results
      dict_imgs = dict()
      while not self.q_out.empty():
        sha1, dict_out = self.q_out.get()
        self.q_out.task_done()
        dict_imgs[sha1] = dict_out

      print "[{}.process_batch: log] Processed update {}. Got features for {} images.".format(self.pp, update_id, len(dict_imgs.keys()))

      # Push them
      self.indexer.push_dict_rows(dict_rows=dict_imgs, table_name=self.indexer.table_sha1infos_name)

      # Mark batch as processed
      update_processed_dict = {
        'update_id': {'info:' + update_str_processed: datetime.now().strftime('%Y-%m-%d:%H.%M.%S')}}
      self.indexer.push_dict_rows(dict_rows=update_processed_dict, table_name=self.indexer.table_updateinfos_name)

      # Cleanup?
      for th in threads:
        del th

  def run(self):
    while True:
      self.process_batch()


if __name__ == "__main__":

  # Get conf file
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  parser.add_argument("-p", "--prefix", dest="prefix", default=default_extr_proc_prefix)
  options = parser.parse_args()

  # Initialize extraction processor
  ep = ExtractionProcessor(options.conf_file, prefix=options.prefix)

  print "Starting extraction {}".format(ep.extr_prefix)
  while True:
    try:
      ep.run()
    except Exception as inst:
      full_trace_error("Extraction processor failed: {}".format(inst))
      time.sleep(10)