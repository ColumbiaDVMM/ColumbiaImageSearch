import json
from datetime import datetime
from argparse import ArgumentParser
from cufacesearch.common.conf_reader import ConfReader
from cufacesearch.indexer.hbase_indexer_minimal import HBaseIndexerMinimal
from cufacesearch.ingester.generic_kafka_processor import GenericKafkaProcessor

default_extr_check_prefix = "EXTR_"

# This class simulates the way updates were generated from the spark workflows but reading from a kafka topic
# Should be run as a single process to ensure data integrity
# Could work with multiple process if update names contains ExtractionChecker id?

class ExtractionChecker(ConfReader):

  def __init__(self, global_conf, prefix=default_extr_check_prefix):
    self.list_extr_prefix = []
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
    self.ingester = GenericKafkaProcessor(self.global_conf, prefix=self.get_required_param("ingester_prefix"))


  def set_pp(self):
    self.pp = "ExtractionChecker"
    self.pp += "_".join(self.list_extr_prefix)

  def store_img_infos(self, msg):
    strk = str(msg['sha1'])
    self.dict_sha1_infos[strk] = dict()
    for k in msg:
      # dumps json of 'img_info'
      if k == "img_info":
        self.dict_sha1_infos[strk][k] = json.dumps(msg[k])
      else:
        # discard 'img_buffer', and 'sha1'
        #if k != "img_buffer" and k != "sha1":
        #  dict_sha1_infos[strk][k] = msg[k]
        # discard 'sha1'
        if k != "sha1":
          self.dict_sha1_infos[strk][k] = msg[k]

  def cleanup_dict_infos(self, list_del_sha1s):
    for sha1 in list_del_sha1s:
      try:
        del self.dict_sha1_infos[str(sha1)]
      except: # could happen when cleaning up duplicates...
        pass

  def get_dict_push(self, list_get_sha1s):
    dict_push = dict()
    update_id, _ = self.indexer.get_next_update_id(today=None, extr_type='_'+self.extr_prefix)
    for sha1 in list_get_sha1s:
      dict_push[str(sha1)] = dict()
      tmp_dict = self.dict_sha1_infos[str(sha1)]
      # build column names properly i.e. appending 'info:'
      for k in tmp_dict:
        dict_push[str(sha1)]['info:' + k] = tmp_dict[k]
      dict_push[str(sha1)][self.batch_check_column] = update_id
    return dict_push, update_id

  def get_unprocessed_rows(self, list_sha1s_to_check):
    # Check if the selected sha1 rows in HBase table 'sha1infos' have those check_column
    # This call will only return rows that DO have those check_column
    sha1s_rows = self.indexer.get_columns_from_sha1_rows(list_sha1s_to_check, self.check_columns,
                                                         families=self.tablesha1_col_families)
    found_sha1_rows = set([str(row[0]) for row in sha1s_rows])
    # Clean up 'dict_sha1_infos' deleting found_sha1_rows
    self.cleanup_dict_infos(found_sha1_rows)
    set_list_sha1s_to_check = set(list_sha1s_to_check)
    unprocessed_rows = set_list_sha1s_to_check - found_sha1_rows
    return unprocessed_rows

  def run(self):
    list_sha1s_to_process = []

    while True:
      list_sha1s_to_check = []

      # Accumulate images infos
      for msg_json in self.ingester.consumer:
        msg = json.loads(msg_json.value)
        list_sha1s_to_check.append(str(msg['sha1']))

        # Store other fields to be able to push them too
        self.store_img_infos(msg)
        if len(list_sha1s_to_check) >= self.indexer.batch_update_size:
          break

      # Check which images have not been processed yet
      unprocessed_rows = self.get_unprocessed_rows(list_sha1s_to_check)

      # Push sha1s to be processed
      for sha1 in unprocessed_rows:
        list_sha1s_to_process.append(sha1)

      # Remove potential duplicates
      list_sha1s_to_process = list(set(list_sha1s_to_process))

      # Push them to HBase by batch of 'batch_update_size'
      # TODO: maybe have also a maximum delay between two pushes?
      # Seems to get stuck with a lag of 1868 total on backpage-test...
      # Then got some HBase errors. No lag anymore but have all images been pushed?
      if len(list_sha1s_to_process) >= self.indexer.batch_update_size:
        print "[{}] Pushing batch of {} images.".format(datetime.now().strftime('%Y-%m-%d:%H.%M.%S'),
                                                        self.indexer.batch_update_size)
        # Trim here to push exactly a batch of 'batch_update_size'
        list_push = list_sha1s_to_process[:self.indexer.batch_update_size]
        # Gather corresponding sha1 infos
        dict_push, update_id = self.get_dict_push(list_push)
        # Push them
        self.indexer.push_dict_rows(dict_push, self.indexer.table_sha1infos_name, families=self.tablesha1_col_families)
        # Push update
        self.indexer.push_list_updates(list_push, update_id)
        # Gather any remaining sha1s and clean up infos
        if len(list_sha1s_to_process) > self.indexer.batch_update_size:
          list_sha1s_to_process = list_sha1s_to_process[self.indexer.batch_update_size:]
        else:
          list_sha1s_to_process = []
        # if duplicates wrt list_push, remove them. Can this still happen?
        list_sha1s_to_process = [sha1 for sha1 in list_sha1s_to_process if sha1 not in list_push]
        self.cleanup_dict_infos(list_push)



if __name__ == "__main__":

  # Get conf file
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  options = parser.parse_args()

  # Initialize extraction checker
  ec = ExtractionChecker(options.conf_file)

  while True:
    try:
      ec.run()
    except Exception as inst:
      print "Extraction checker failed {}".format(inst)
