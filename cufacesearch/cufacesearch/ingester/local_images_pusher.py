from __future__ import print_function

import sys
import json
import time
from argparse import ArgumentParser
from cufacesearch.common.conf_reader import ConfReader
# TODO: separate consumer/producer
#from cufacesearch.ingester.generic_kafka_processor import GenericKafkaProcessor
#from cufacesearch.ingester.kakfa_producer import KafkaProducer
from cufacesearch.ingester.kinesis_producer import KinesisProducer


#default_prefix = "LIKP_"
default_prefix = "LIP_"
skip_formats = ['SVG', 'RIFF']
valid_formats = ['JPEG', 'JPG', 'GIF', 'PNG']

class LocalImagePusher(ConfReader):
  # To push list of images to be processed from the folder 'input_path' containing the images

  def __init__(self, global_conf, prefix=default_prefix):
    super(LocalImagePusher, self).__init__(global_conf, prefix)

    self.set_pp(pp="LocalImagePusher")

    # Input parameters and members
    self.input_path = self.get_required_param('input_path')
    self.source_zip = self.get_param('source_zip')
    self.ingested_images = set()
    self.process_count = 0
    self.process_skip = 0
    self.process_failed = 0
    self.process_time = 0
    self.display_count = self.get_param('display_count', 100)

    # any additional initialization needed, like producer specific output logic
    self.producer = None
    self.images_out_topic = None
    self.init_producer()

  def init_producer(self):
    # TODO: should check for producer_type: Kafka or Kinesis.
    # Read all required parameters and prepare producer
    self.producer_type = self.get_required_param('producer_type')
    producer_prefix = self.get_required_param('producer_prefix')
    if self.producer_type == "kafka":
      # TODO: What are needed parameters here?
      # Should we pass whole configuration?
      #self.producer = GenericKafkaProcessor()
      #self.producer = KafkaProducer(self.global_conf, prefix=producer_prefix)
      raise ValueError("[{}: ERROR] KafkaProducer not yet supported!".format(self.pp))
    elif self.producer_type == "kinesis":
      # TODO: What are needed parameters here?
      # Should we pass whole configuration?
      self.producer = KinesisProducer(self.global_conf, prefix=producer_prefix)
      self.images_out_topic = self.producer.stream_name
    else:
      msg = "[{}: ERROR] Unknown producer type: {}"
      raise ValueError(msg.format(self.pp, self.producer_type))

  def get_next_img(self):
    # TODO: test that
    #  improvement: with a timestamp ordering? to try to add automatically new images?
    #  we could also not rely on file extension but try to actually get the type of the image.
    import os
    for root, dirs, files in os.walk(self.input_path):
      for basename in files:
        if basename.split('.')[-1].upper() in valid_formats:
          filename = os.path.join(root, basename)
          if filename not in self.ingested_images:
            yield filename
            self.ingested_images.add(filename)

  def build_image_msg(self, dict_imgs):
    # Build dict ouput for each image with fields 'img_path', 'sha1', 'img_info'
    img_out_msgs = []
    for img_path in dict_imgs:
      tmp_dict_out = dict()
      # TODO: use indexer.img_path_column.split(':')[-1] instead of 'img_path'?
      # Should the img_path be relative to self.input_path?
      tmp_dict_out['img_path'] = img_path
      tmp_dict_out['sha1'] = dict_imgs[img_path]['sha1']
      tmp_dict_out['img_info'] = dict_imgs[img_path]['img_info']
      img_out_msgs.append(json.dumps(tmp_dict_out).encode('utf-8'))
    return img_out_msgs

  def toc_process_ok(self, start_process, end_time=None):
    """Log one process completed

    :param start_process: start time of process (in seconds since epoch)
    :type start_process: int
    :param end_time: end time of process, if 'None' will be set to now (in seconds since epoch)
    :type end_time: int
    """
    if end_time is None:
      end_time = time.time()
    self.process_time += abs(end_time - start_process)
    self.process_count += 1

  def toc_process_skip(self, start_process, end_time=None):
    """Log one skipped process

    :param start_process: start time of process (in seconds since epoch)
    :type start_process: int
    :param end_time: end time of process, if 'None' will be set to now (in seconds since epoch)
    :type end_time: int
    """
    if end_time is None:
      end_time = time.time()
    self.process_time += abs(end_time - start_process)
    self.process_skip += 1

  def toc_process_failed(self, start_process, end_time=None):
    """Log one process failed

    :param start_process: start time of process (in seconds since epoch)
    :type start_process: int
    :param end_time: end time of process, if 'None' will be set to now (in seconds since epoch)
    :type end_time: int
    """
    if end_time is None:
      end_time = time.time()
    self.process_time += abs(end_time - start_process)
    self.process_failed += 1

  def print_push_stats(self):
    # How come self.process_time is negative?
    avg_process_time = self.process_time / max(1, self.process_count + self.process_failed)
    msg = "[{}: log] push count: {}, failed: {}, avg. time: {}"
    print(msg.format(self.pp, self.process_count, self.process_failed, avg_process_time))

  def process(self):
    from cufacesearch.imgio.imgio import get_SHA1_img_info_from_buffer, get_buffer_from_filepath
    nb_img_found = 0

    if self.producer is None:
      raise ValueError("Producer was not initialized, will not be able to push. Is Kafka ready or reachable?")

    # Get images data and infos
    for img_path in self.get_next_img():
      nb_img_found += 1
      start_process = time.time()

      if (self.process_count + self.process_failed) % self.display_count == 0:
        self.print_push_stats()

      dict_imgs = dict()
      # Could we multi-thread that?

      if self.verbose > 4:
        msg = "[{}.process_one: info] Reading image from: {}"
        print(msg.format(self.pp, img_path))
      try:
        img_buffer = get_buffer_from_filepath(img_path)
        if img_buffer:
          sha1, img_type, width, height = get_SHA1_img_info_from_buffer(img_buffer)
          dict_imgs[img_path] = {'img_buffer': img_buffer, 'sha1': sha1,
                            'img_info': {'format': img_type, 'width': width, 'height': height}}
          self.toc_process_ok(start_process, end_time=time.time())
        else:
          self.toc_process_failed(start_process, end_time=time.time())
          if self.verbose > 1:
            msg = "[{}.process_one: info] Could not read image from: {}"
            print(msg.format(self.pp, img_path))
      except Exception as inst:
        self.toc_process_failed(start_process, end_time=time.time())
        if self.verbose > 0:
          msg = "[{}.process_one: error] Could not read image from: {} ({})"
          print(msg.format(self.pp, img_path, inst))

      # Push to images_out_topic
      for img_out_msg in self.build_image_msg(dict_imgs):
        self.producer.send(self.images_out_topic, img_out_msg)

    if nb_img_found > 0:
      self.print_push_stats()
      msg = "[{}: log] Found {} new images in: {}. Total: {}."
      print(msg.format(self.pp, nb_img_found, self.input_path, len(self.ingested_images)))

    sys.stdout.flush()


if __name__ == "__main__":
  # Get config
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  options = parser.parse_args()

  lip = LocalImagePusher(options.conf_file)
  if lip.source_zip:
    import os, sys
    from cufacesearch.common.dl import download_file, untar_file
    local_zip = os.path.join(lip.input_path, lip.source_zip.split('/')[-1])
    if not os.path.exists(local_zip):
      print("Downloading {} to {}".format(lip.source_zip, local_zip))
      sys.stdout.flush()
      download_file(lip.source_zip, local_zip)
      untar_file(local_zip, lip.input_path)

  while True:
    lip.process()
    time.sleep(60)