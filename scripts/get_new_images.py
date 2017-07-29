# Look for images that have been indexed after a given date to catch up in terms of face detection.
# Prepare data for processing with spark workflow

from cufacesearch.indexer.hbase_indexer_minimal import HBaseIndexerMinimal
from argparse import ArgumentParser

default_conf = "../conf/global_conf_get_new_face_hbase.json"

if __name__ == "__main__":

  # get options
  parser = ArgumentParser()
  parser.add_argument("-s", "--start_date", dest="start_date", required=True)
  parser.add_argument("-c", "--conf", dest="conf", default=default_conf)

  opts = parser.parse_args()

  # Create indexer
  hbim = HBaseIndexerMinimal(opts.conf)
  rows = hbim.get_updates_from_date(opts.start_date)

  print rows