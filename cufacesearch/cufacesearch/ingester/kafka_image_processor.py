import ArgumentParser
from .generic_kafka_processor import GenericKafkaProcessor

default_prefix = "KIP_"

# should we inherit from multiprocessing.Process as in
# https://github.com/dpkp/kafka-python/blob/master/example.py
class KafkaImageProcessor(GenericKafkaProcessor):

  def __init__(self, conf, prefix=default_prefix):
    # call GenericKafkaProcessor init (and others potentially)
    super(KafkaImageProcessor, self).__init__(conf, prefix)
    # any additional initialization needed, like producer specific output logic
    self.cdr_out_topic = self.get_required_param('cdr_out_topic')
    self.images_out_topic = self.get_required_param('images_out_topic')
    # TODO: also get s3 url prefix

  def set_pp(self):
    self.pp = "KafkaImageProcessor"

  def get_images_urls(self, msg):
    # TODO: filter 'objects' array based on 'content_type' to get only images urls and prepend prefix
    return []
  
  def build_cdr_msg(self, msg, dict_imgs):
    # TODO: edit 'objects' array to add 'img_infos', and 'img_sha1' for images
    return ""

  def build_image_msg(self, dict_imgs):
    # Build list of tuple (sha1, s3_url, img_infos, img_buffer)
    img_out_msgs = []
    for url in dict_imgs:
      img_out_msgs.append((dict_imgs[url]['sha1'], url, dict_imgs[url]['img_infos'], dict_imgs[url]['img_buffer']),)
    return img_out_msgs


  def process_one(self, msg):
    from ..imgio.imgio import get_SHA1_img_info_from_buffer, get_buffer_from_URL
    print ("%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition,
                                          msg.offset, msg.key,
                                          msg.value))

    # From msg value get list_urls for image objects only
    list_urls = self.get_images_urls(msg)

    # Get images data and infos
    dict_imgs = dict()
    for url in list_urls:
      img_buffer = get_buffer_from_URL(url)
      sha1, img_type, width, height = get_SHA1_img_info_from_buffer(img_buffer)
      dict[url] = {'buffer': img_buffer, 'sha1': sha1, 'img_infos': {'format': img_type, 'width': width, 'height': height}}

    # Push to cdr_out_topic
    self.producer.send(self.cdr_out_topic, self.build_cdr_msg(msg, dict_imgs))

    # Push to images_out_topic
    for img_out_msg in self.build_image_msg(dict_imgs):
      self.producer.send(self.images_out_topic, img_out_msg)


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
