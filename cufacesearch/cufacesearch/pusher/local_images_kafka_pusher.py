#DEPRECATED
import json
import time
from argparse import ArgumentParser
from cufacesearch.ingester.generic_kafka_processor import GenericKafkaProcessor

default_prefix = "LIKP_"
skip_formats = ['SVG', 'RIFF']
valid_formats = ['JPEG', 'JPG', 'GIF', 'PNG']

class LocalImageKafkaPusher(GenericKafkaProcessor):
  # To push list of images to be processed from the folder 'input_path' containing the images

  def __init__(self, global_conf_filename, prefix=default_prefix, pid=None):
    # call GenericKafkaProcessor init (and others potentially)
    super(LocalImageKafkaPusher, self).__init__(global_conf_filename, prefix, pid)
    # any additional initialization needed, like producer specific output logic
    self.images_out_topic = self.get_required_param('producer_images_out_topic')
    self.input_path = self.get_required_param('input_path')
    self.source_zip = self.get_param('source_zip')
    self.ingested_images = set()

    self.set_pp(pp="LocalImageKafkaPusher")

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

  def print_push_stats(self):
    # How come self.process_time is negative?
    avg_process_time = self.process_time / max(1, self.process_count + self.process_failed)
    print_msg = "[%s: log] push count: %d, failed: %d, avg. time: %f"
    print print_msg % (self.pp, self.process_count, self.process_failed, avg_process_time)

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
        print_msg = "[{}.process_one: info] Reading image from: {}"
        print print_msg.format(self.pp, img_path)
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
            print_msg = "[{}.process_one: info] Could not read image from: {}"
            print print_msg.format(self.pp, img_path)
      except Exception as inst:
        self.toc_process_failed(start_process, end_time=time.time())
        if self.verbose > 0:
          print_msg = "[{}.process_one: error] Could not read image from: {} ({})"
          print print_msg.format(self.pp, img_path, inst)

      # Push to images_out_topic
      for img_out_msg in self.build_image_msg(dict_imgs):
        self.producer.send(self.images_out_topic, img_out_msg)

    if nb_img_found > 0:
      self.print_push_stats()
      print "[{}: log] Found {} new images in: {}. Total: {}.".format(self.pp, nb_img_found, self.input_path,
                                                                    len(self.ingested_images))
    sys.stdout.flush()

if __name__ == "__main__":
  # Get config
  parser = ArgumentParser()
  parser.add_argument("-c", "--conf", dest="conf_file", required=True)
  options = parser.parse_args()

  likp = LocalImageKafkaPusher(options.conf_file)
  if likp.source_zip:
    import os, sys
    from cufacesearch.common.dl import download_file, untar_file
    local_zip = os.path.join(likp.input_path, likp.source_zip.split('/')[-1])
    if not os.path.exists(local_zip):
      print "Downloading {} to {}".format(likp.source_zip, local_zip)
      sys.stdout.flush()
      download_file(likp.source_zip, local_zip)
      untar_file(local_zip, likp.input_path)

  while True:
    likp.process()
    time.sleep(60)