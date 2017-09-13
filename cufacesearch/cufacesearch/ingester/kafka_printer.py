import sys
from .generic_kafka_processor import GenericKafkaProcessor

default_printer_prefix = "KPRINT_"

class KafkaPrinter(GenericKafkaProcessor):
  def __init__(self, global_conf_filename, prefix=default_printer_prefix, pid=None):
    # call GenericKafkaProcessor init (and others potentially)
    super(KafkaPrinter, self).__init__(global_conf_filename, prefix, pid)

    self.max_print = 10
    param_max_print = self.get_param('max_print')
    if param_max_print:
      self.max_print = param_max_print

    self.print_fields = None
    print_fields = self.get_param('print_fields')
    if print_fields:
      self.print_fields = print_fields

    self.count_print = 0

    # Set print prefix
    self.set_pp()

  def set_pp(self):
    self.pp = "KafkaPrinter"
    if self.pid:
      self.pp += "." + str(self.pid)

  def process_one(self, msg):
    if self.count_print >= self.max_print:
      print "[{}] Reached maximum number of print out. Leaving.".format(self.pp)
      exit(0)
    import json
    msg_value = json.loads(msg.value)
    msg_print = []
    if self.print_fields is not None:
      for field in self.print_fields:
        if field in msg_value:
          msg_print.append(msg_value[field])
      #print msg_print
    else:
      msg_print = [json.dumps(msg_value)]
    print "[{}: msg] keys: {}, data: {}".format(self.pp, msg_value.keys(), msg_print)
    sys.stdout.flush()
    self.count_print += 1

  def init_producer(self):
    # No results for printer
    pass