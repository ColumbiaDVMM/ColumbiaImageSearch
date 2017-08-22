from argparse import ArgumentParser
from cufacesearch.ingester.kafka_face_processor import KafkaFaceProcessor, DeamonKafkaFaceProcessor, default_prefix

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
      print "Starting DeamonKafkaFaceProcessor worker #{}".format(w)
      dkip = DeamonKafkaFaceProcessor(options.conf_file, prefix=options.prefix)
      dkip.start()
      # How should we exit properly?
  else:
    # Initialize
    kfp = KafkaFaceProcessor(options.conf_file)

    # Ingest
    while True:
      for msg in kfp.consumer:
        kfp.process_one(msg)