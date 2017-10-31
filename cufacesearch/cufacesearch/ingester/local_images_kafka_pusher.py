import json
import time

from .generic_kafka_processor import GenericKafkaProcessor

default_prefix = "LIKP_"
skip_formats = ['SVG', 'RIFF']

class LocalImageKafkaPusher(GenericKafkaProcessor):
  # To push list of images to be processed from the folder 'input_path' containing the images

  def __init__(self, global_conf_filename, prefix=default_prefix, pid=None):
    # call GenericKafkaProcessor init (and others potentially)
    super(LocalImageKafkaPusher, self).__init__(global_conf_filename, prefix, pid)
    # any additional initialization needed, like producer specific output logic
    self.images_out_topic = self.get_required_param('producer_cdr_out_topic')
    self.input_path = self.get_required_param('input_path')

    self.set_pp()

  def set_pp(self):
    self.pp = "LocalImageKafkaPusher"

  def get_next_img(self):
    # TODO: could use glob. Actually implement and test that
    #  with a timestamp ordering? to try to add automatically new images?
    # should we define a pattern to be matched i.e. valid image file extension?
    import glob
    yield glob.iglob(self.input_path)

  def build_image_msg(self, dict_imgs):
    # Build dict ouput for each image with fields 's3_url', 'sha1', 'img_info' and 'img_buffer'
    img_out_msgs = []
    for img_path in dict_imgs:
      tmp_dict_out = dict()
      # TODO: use indexer.img_path_column.split(':')[-1] instead of img_path?
      tmp_dict_out['img_path'] = img_path
      tmp_dict_out['sha1'] = dict_imgs[img_path]['sha1']
      tmp_dict_out['img_info'] = dict_imgs[img_path]['img_info']
      img_out_msgs.append(json.dumps(tmp_dict_out).encode('utf-8'))
    return img_out_msgs

  def process(self):
    from ..imgio.imgio import get_SHA1_img_info_from_buffer, get_buffer_from_filepath

    # Get images data and infos
    for sha1, img_path in self.get_next_img():

      if (self.process_count + self.process_failed) % self.display_count == 0:
        avg_process_time = self.process_time / max(1, self.process_count + self.process_failed)
        print_msg = "[%s] dl count: %d, failed: %d, time: %f"
        print print_msg % (self.pp, self.process_count, self.process_failed, avg_process_time)

      dict_imgs = dict()
      # Could we multi-thread that?
      start_process = time.time()
      if self.verbose > 2:
        print_msg = "[{}.process_one: info] Reading image from: {}"
        print print_msg.format(self.pp, img_path)
      try:
        img_buffer = get_buffer_from_filepath(img_path)
        if img_buffer:
          sha1, img_type, width, height = get_SHA1_img_info_from_buffer(img_buffer)
          dict_imgs[img_path] = {'img_buffer': img_buffer, 'sha1': sha1,
                            'img_info': {'format': img_type, 'width': width, 'height': height}}
          self.toc_process_ok(start_process)
        else:
          self.toc_process_failed(start_process)
          if self.verbose > 1:
            print_msg = "[{}.process_one: info] Could not read image from: {}"
            print print_msg.format(self.pp, img_path)
      except Exception as inst:
        self.toc_process_failed(start_process)
        if self.verbose > 0:
          print_msg = "[{}.process_one: error] Could not read image from: {} ({})"
          print print_msg.format(self.pp, img_path, inst)

      # Push to images_out_topic
      # Beware, this pushes a LOT of data to the Kafka topic self.images_out_topic...
      for img_out_msg in self.build_image_msg(dict_imgs):
        self.producer.send(self.images_out_topic, img_out_msg)


