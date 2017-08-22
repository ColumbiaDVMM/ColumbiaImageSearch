from argparse import ArgumentParser
from cufacesearch.ingester.kafka_image_processor import KafkaImageProcessor, DeamonKafkaImageProcessor, default_prefix

if __name__ == "__main__":

  # Get conf file
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  parser.add_argument("-p", "--prefix", dest="prefix", default=default_prefix)
  parser.add_argument("-d", "--deamon", dest="deamon", action="store_true", default=False)
  parser.add_argument("-w", "--workers", dest="workers", type=int, default=8)
  options = parser.parse_args()

  if options.deamon:  # use daemon
    for w in range(options.workers):
      print "Starting DeamonKafkaImageProcessor worker #{}".format(w)
      dkip = DeamonKafkaImageProcessor(options.conf_file, prefix=options.prefix)
      dkip.start()
    # How should we exit properly?
  else:
    # Initialize
    kip = KafkaImageProcessor(options.conf_file, prefix=options.prefix)

    # Ingest
    while True:
      for msg in kip.consumer:
        kip.process_one(msg)

