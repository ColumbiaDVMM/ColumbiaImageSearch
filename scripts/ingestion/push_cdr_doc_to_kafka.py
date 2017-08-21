from argparse import ArgumentParser
from cufacesearch.ingester.kafka_cdr_ingester import CDRIngester, default_prefix

if __name__ == "__main__":

  # Get conf file
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  parser.add_argument("-p", "--prefix", dest="prefix", default=default_prefix)
  options = parser.parse_args()

  # Initialize
  cdri = CDRIngester(options.conf_file, prefix=options.prefix)

  while True:
    cdri.push_batch()
    raw_input("Press Enter to continue...")
