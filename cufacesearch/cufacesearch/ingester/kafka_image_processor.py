from .generic_kafka_processor import GenericKafkaProcessor
import multiprocessing

default_prefix = "KIP_"

# should we inherit from multiprocessing.Process as in
# https://github.com/dpkp/kafka-python/blob/master/example.py
class KafkaImageProcessor(GenericKafkaProcessor):

  def __init__(self, global_conf_filename, prefix=default_prefix):
    # call GenericKafkaProcessor init (and others potentially)
    super(KafkaImageProcessor, self).__init__(global_conf_filename, prefix)
    # any additional initialization needed, like producer specific output logic
    self.cdr_out_topic = self.get_required_param('cdr_out_topic')
    self.images_out_topic = self.get_required_param('images_out_topic')
    # TODO: also get s3 url prefix
    # for now "object_stored_prefix" in "_meta" of domain CDR
    self.url_prefix = ""

  def set_pp(self):
    self.pp = "KafkaImageProcessor"

  def get_images_urls(self, msg):
    # Filter 'objects' array based on 'content_type' to get only images urls
    list_urls = []
    if 'objects' in msg.value:
      for obj_pos, obj_val in msg.value['objects']:
        if 'content_type' in obj_val and obj_val['content_type'].startswith("image"):
          # prepend prefix so images URLs are actually valid
          image_url = self.url_prefix + obj_val['obj_stored_url']
          # need to keep obj_pos for alignment
          list_urls.append((image_url, obj_pos))
    else:
      print "[{}.get_images_urls: info] Message had not 'objects' fields.".format(self.pp)

    return list_urls
  
  def build_cdr_msg(self, msg, dict_imgs):
    # Edit 'objects' array to add 'img_infos', and 'img_sha1' for images
    for url in dict_imgs:
      img = dict_imgs[url]
      tmp_obj = msg.value['objects'][img['obj_pos']]
      tmp_obj['img_infos'] = img['img_infos']
      tmp_obj['img_sha1'] = img['sha1']
      msg.value['objects'][img['obj_pos']] = tmp_obj
    # should we return just the value?
    return msg

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
    for url, obj_pos in list_urls:
      img_buffer = get_buffer_from_URL(url)
      sha1, img_type, width, height = get_SHA1_img_info_from_buffer(img_buffer)
      dict_imgs[url] = {'obj_pos': obj_pos, 'buffer': img_buffer, 'sha1': sha1, 'img_infos': {'format': img_type, 'width': width, 'height': height}}

    # Push to cdr_out_topic
    self.producer.send(self.cdr_out_topic, self.build_cdr_msg(msg, dict_imgs))

    # Push to images_out_topic
    for img_out_msg in self.build_image_msg(dict_imgs):
      self.producer.send(self.images_out_topic, img_out_msg)

# TODO: test deamon
class DeamonKafkaImageProcessor(multiprocessing.Process):

  daemon = True

  def __init__(self, conf, prefix=default_prefix):
    super(DeamonKafkaImageProcessor, self).__init__()
    self.conf = conf
    self.prefix = prefix

  def run(self):

    kip = KafkaImageProcessor(self.conf, prefix=self.prefix)

    for msg in kip.consumer:
      kip.process_one(msg)
