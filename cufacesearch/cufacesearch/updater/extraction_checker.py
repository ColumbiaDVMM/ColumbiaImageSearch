import os
import sys
import json
import time
import traceback
import multiprocessing
from datetime import datetime
from argparse import ArgumentParser
from cufacesearch.common.conf_reader import ConfReader
from cufacesearch.indexer.hbase_indexer_minimal import HBaseIndexerMinimal
from cufacesearch.ingester.generic_kafka_processor import GenericKafkaProcessor

default_extr_check_prefix = "EXTR_"

# This class simulates the way updates were generated from the spark workflows but reading from a kafka topic
# Should be run as a single process to ensure data integrity

# We could have a process that looks for updates that were created (or even started) a long time ago but not finished
# and repush them to the updates topic...

class ExtractionChecker(ConfReader):

  def __init__(self, global_conf, prefix=default_extr_check_prefix, pid=None):
    self.list_extr_prefix = []
    self.pid = pid
    self.dict_sha1_infos = dict()

    super(ExtractionChecker, self).__init__(global_conf, prefix)


    self.featurizer_type = self.get_required_param("featurizer_type")
    self.detector_type = self.get_required_param("detector_type")
    self.input_type = self.get_required_param("input_type")

    # Need to be build from extraction type and detection input + "_processed"
    self.extr_family_column = "ext"
    tmp_extr_family_column = self.get_param("extr_family_column")
    if tmp_extr_family_column:
      self.extr_family_column = tmp_extr_family_column

    # Max delay
    self.max_delay = 3600
    #self.max_delay = 600
    max_delay = self.get_param("max_delay")
    if max_delay:
      self.max_delay = int(max_delay)
    self.last_push = time.time()

    # Beware, the self.extr_family_column should be added to the indexer families parameter in get_create_table...
    self.tablesha1_col_families = {'info': dict(), self.extr_family_column: dict()}
    self.list_extr_prefix = [self.featurizer_type, "feat", self.detector_type, self.input_type]
    self.extr_prefix = "_".join(self.list_extr_prefix)
    self.extr_prefix_base_column_name = self.extr_family_column + ":" + self.extr_prefix
    self.extr_check_column = self.extr_prefix_base_column_name + "_processed"
    # Need to be build from extraction type and extraction input + "_batchid"
    self.batch_check_column = self.extr_prefix_base_column_name + "_updateid"
    self.check_columns = [self.extr_check_column, self.batch_check_column]

    self.set_pp()

    # Initialize indexer and ingester
    self.indexer = HBaseIndexerMinimal(self.global_conf, prefix=self.get_required_param("indexer_prefix"))
    self.ingester = GenericKafkaProcessor(self.global_conf, prefix=self.get_required_param("check_ingester_prefix"))
    self.updates_out_topic = self.ingester.get_required_param("producer_updates_out_topic")
    self.ingester.pp = "ec"
    if self.pid:
      self.ingester.pp += str(self.pid)


  def set_pp(self):
    self.pp = "ExtractionChecker."
    self.pp += "-".join(self.list_extr_prefix)
    if self.pid:
      self.pp += "."+str(self.pid)

  def store_img_infos(self, msg):
    strk = str(msg['sha1'])
    self.dict_sha1_infos[strk] = dict()
    for k in msg:
      # dumps json of 'img_info'
      if k == "img_info":
        self.dict_sha1_infos[strk][k] = json.dumps(msg[k])
      else:
        # discard 'img_buffer' (if it exists?...), and 'sha1'
        #if k != "img_buffer" and k != "sha1":
        #  self.dict_sha1_infos[strk][k] = msg[k]
        # discard 'sha1'
        if k != "sha1":
          self.dict_sha1_infos[strk][k] = msg[k]

  def cleanup_dict_infos(self, list_del_sha1s):
    for sha1 in list_del_sha1s:
      try:
        del self.dict_sha1_infos[str(sha1)]
      except:
        # could happen when cleaning up duplicates or image processed by another process
        pass

  def get_dict_push(self, list_get_sha1s):
    dict_push = dict()
    # append unique processid to 'update_id' to make it safe to use with multiple consumers...
    # /!\ beware, it should not contain underscores
    # use self.ingester.pp
    tmp_update_id, _ = self.indexer.get_next_update_id(today=None, extr_type=self.extr_prefix)
    # Beware: after a restart forming update_id like this may make it not unique i.e same as one before restart...
    #update_id = tmp_update_id+'-'+self.ingester.pp
    # Add time
    update_id = tmp_update_id+'-'+self.ingester.pp+'-'+str(time.time())
    for sha1 in list_get_sha1s:
      dict_push[str(sha1)] = dict()
      try:
        tmp_dict = self.dict_sha1_infos[str(sha1)]
      except:
        # This would mean the image has been marked as part of another batch by another process,
        # and thus deleted in a previous 'get_unprocessed_rows' call
        del dict_push[str(sha1)]
        continue
      # build column names properly i.e. appending 'info:'
      for k in tmp_dict:
        dict_push[str(sha1)]['info:' + k] = tmp_dict[k]
      dict_push[str(sha1)][self.batch_check_column] = update_id
    return dict_push, update_id

  def get_unprocessed_rows(self, list_sha1s_to_check):
    unprocessed_rows = set(list_sha1s_to_check)

    if list_sha1s_to_check:
      # Check if the selected sha1 rows in HBase table 'sha1infos' have those check_column
      # This call will only return rows that DO have those check_column
      sha1s_rows = self.indexer.get_columns_from_sha1_rows(list_sha1s_to_check, self.check_columns,
                                                           families=self.tablesha1_col_families)
      if sha1s_rows:
        found_sha1_rows = set([str(row[0]) for row in sha1s_rows])
        # Clean up 'dict_sha1_infos' deleting found_sha1_rows
        # Beware, this can be dangerous in multiprocess setting...
        self.cleanup_dict_infos(found_sha1_rows)
        set_list_sha1s_to_check = set(list_sha1s_to_check)
        unprocessed_rows = set_list_sha1s_to_check - found_sha1_rows

    return unprocessed_rows

  def run(self):
    try:
      list_sha1s_to_process = []

      while True:
        list_sha1s_to_check = []

        try:
          # Accumulate images infos
          for msg_json in self.ingester.consumer:
            msg = json.loads(msg_json.value)
            list_sha1s_to_check.append(str(msg['sha1']))

            # Store other fields to be able to push them too
            self.store_img_infos(msg)

            if len(list_sha1s_to_check) >= self.indexer.batch_update_size:
              break
        except Exception as inst:
          # trying to use 'consumer_timeout_ms' to raise timeout and get last samples
          warn_msg = "[{}: warning] At {}, caught {} {} in consumer loop"
          print warn_msg.format(self.pp, datetime.now().strftime('%Y-%m-%d:%H.%M.%S'), type(inst), inst)
          sys.stdout.flush()

        # Check which images have not been processed (or pushed in an update) yet
        unprocessed_rows = self.get_unprocessed_rows(list_sha1s_to_check)

        # Push sha1s to be processed
        for sha1 in unprocessed_rows:
          list_sha1s_to_process.append(sha1)

        # Remove potential duplicates
        list_sha1s_to_process = list(set(list_sha1s_to_process))

        if list_sha1s_to_process:
          # Push them to HBase by batch of 'batch_update_size'
          # Seems to get stuck with a lag of 1868 total on backpage-test...
          # Then got some HBase errors. No lag anymore but have all images been pushed?
          if len(list_sha1s_to_process) >= self.indexer.batch_update_size or ((time.time() - self.last_push) > self.max_delay and len(list_sha1s_to_process) > 0):
            # Trim here to push exactly a batch of 'batch_update_size'
            list_push = list_sha1s_to_process[:min(self.indexer.batch_update_size, len(list_sha1s_to_process))]

            # Gather corresponding sha1 infos
            dict_push, update_id = self.get_dict_push(list_push)
            push_msg = "[{}: at {}] Pushing update {} of {} images."
            print push_msg.format(self.pp, datetime.now().strftime('%Y-%m-%d:%H.%M.%S'), update_id, len(dict_push.keys()))
            sys.stdout.flush()

            # Push them
            self.indexer.push_dict_rows(dict_push, self.indexer.table_sha1infos_name, families=self.tablesha1_col_families)
            # Push update
            #self.indexer.push_list_updates(list_push, update_id)
            self.indexer.push_list_updates(dict_push.keys(), update_id)
            # We also push update_id and list_push to a kafka topic to allow better parallelized extraction
            dict_updates = dict()
            dict_updates[update_id] = ','.join(dict_push.keys())
            self.ingester.producer.send(self.updates_out_topic, json.dumps(dict_updates))

            # Gather any remaining sha1s and clean up infos
            if len(list_sha1s_to_process) > self.indexer.batch_update_size:
              list_sha1s_to_process = list_sha1s_to_process[self.indexer.batch_update_size:]
            else:
              list_sha1s_to_process = []
            # if duplicates wrt list_push, remove them. Can this still happen?
            list_sha1s_to_process = [sha1 for sha1 in list_sha1s_to_process if sha1 not in list_push]
            self.cleanup_dict_infos(list_push)
            self.last_push = time.time()

    except Exception as inst:
      exc_type, exc_obj, exc_tb = sys.exc_info()
      fulltb = traceback.format_tb(exc_tb)
      raise type(inst)(" {} ({})".format(inst, ''.join(fulltb)))

class DaemonExtractionChecker(multiprocessing.Process):
  daemon = True

  def __init__(self, conf, prefix=default_extr_check_prefix):
    super(DaemonExtractionChecker, self).__init__()
    self.conf = conf
    self.prefix = prefix

  def run(self):
    nb_death = 0
    while True:
      try:
        print "Starting worker ExtractionChecker.{}".format(self.pid)
        ec = ExtractionChecker(self.conf, prefix=self.prefix, pid=self.pid)
        ec.run()
      except Exception as inst:
        nb_death += 1
        #exc_type, exc_obj, exc_tb = sys.exc_info()
        #fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print "ExtractionChecker.{} died {}{}".format(self.pid, type(inst), inst)
        time.sleep(10*nb_death)




if __name__ == "__main__":

  # Get conf file
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  parser.add_argument("-p", "--prefix", dest="prefix", default=default_extr_check_prefix)
  parser.add_argument("-d", "--deamon", dest="deamon", action="store_true", default=False)
  parser.add_argument("-w", "--workers", dest="workers", type=int, default=1)
  options = parser.parse_args()

  if options.deamon:  # use daemon
    for w in range(options.workers):
      dec = DaemonExtractionChecker(options.conf_file, prefix=options.prefix)
      dec.start()
  else:
    # Initialize extraction checker
    ec = ExtractionChecker(options.conf_file)
    nb_death = 0
    while True:
      try:
        ec.run()
      except Exception as inst:
        print "Extraction checker failed {}".format(inst)
        time.sleep(10 * nb_death)
        nb_death += 1
