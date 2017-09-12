from argparse import ArgumentParser
from cufacesearch.ingester.kafka_printer import KafkaPrinter, default_printer_prefix

if __name__ == "__main__":

  # Get conf file
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  parser.add_argument("-p", "--prefix", dest="prefix", default=default_printer_prefix)
  options = parser.parse_args()

  print options

  kp = KafkaPrinter(options.conf_file, prefix=options.prefix)

  # Ingest
  for msg in kp.consumer:
    kp.process_one(msg)
