from argparse import ArgumentParser
from cufacesearch.ingester.kafka_image_processor import KafkaImageProcessor, DaemonKafkaImageProcessor, default_prefix
import time

if __name__ == "__main__":

  # Get conf file
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  parser.add_argument("-p", "--prefix", dest="prefix", default=default_prefix)
  parser.add_argument("-d", "--deamon", dest="deamon", action="store_true", default=False)
  parser.add_argument("-w", "--workers", dest="workers", type=int, default=8)
  parser.add_argument("-m", "--max_message", dest="max_message", type=int, default=0)
  options = parser.parse_args()

  nb_msg = 0

  if options.deamon:  # use daemon
    for w in range(options.workers):
      print "Starting DaemonKafkaImageProcessor worker #{}".format(w)
      dkip = DaemonKafkaImageProcessor(options.conf_file, prefix=options.prefix)
      dkip.start()
    # How should we exit properly?
  else:
    # Initialize
    kip = KafkaImageProcessor(options.conf_file, prefix=options.prefix)

    # Ingest
    while True:
      for msg in kip.consumer:
        kip.process_one(msg)
        nb_msg += 1
        if options.max_message > 0 and nb_msg >= options.max_message:
          time.sleep(10)
          exit()
