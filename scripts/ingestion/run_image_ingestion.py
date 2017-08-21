from argparse import ArgumentParser
from cufacesearch.ingester.kafka_image_processor import KafkaImageProcessor, default_prefix

if __name__ == "__main__":

  # Get conf file
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  parser.add_argument("-p", "--prefix", dest="prefix", default=default_prefix)
  options = parser.parse_args()

  # Initialize
  kip = KafkaImageProcessor(options.conf_file, prefix=options.prefix)

  # Ingest
  while True:
    for msg in kip.consumer:
      kip.process_one(msg)

  # Should we use Daemon instead?