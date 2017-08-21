from argparse import ArgumentParser
from cufacesearch.ingester.kafka_face_processor import KafkaFaceProcessor, default_prefix


if __name__ == "__main__":

  # Get conf file
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  parser.add_argument("-p", "--prefix", dest="prefix", default=default_prefix)
  options = parser.parse_args()

  # Initialize
  kfp = KafkaFaceProcessor(options.conf_file)

  # Ingest
  while True:
    for msg in kfp.consumer:
      kfp.process_one(msg)