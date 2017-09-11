import os
import sys
import json
import time
import threading
import multiprocessing
from .generic_kafka_processor import GenericKafkaProcessor
from ..imgio.imgio import buffer_to_B64

default_prefix = "KID_"
default_prefix_frompkl = "KIDFP_"


class KafkaImageDownloader(GenericKafkaProcessor):
  def __init__(self, global_conf_filename, prefix=default_prefix, pid=None):
    # when running as deamon
    self.pid = pid
    # call GenericKafkaProcessor init (and others potentially)
    super(KafkaImageDownloader, self).__init__(global_conf_filename, prefix)
    # any additional initialization needed, like producer specific output logic
    self.cdr_out_topic = self.get_required_param('producer_cdr_out_topic')
    self.images_out_topic = self.get_required_param('producer_images_out_topic')
    # TODO: get s3 url prefix from actual location
    # for now "object_stored_prefix" in "_meta" of domain CDR
    # but just get from conf
    self.url_prefix = self.get_required_param('obj_stored_prefix')

    # Set print prefix
    self.set_pp()

  def set_pp(self):
    self.pp = "KafkaImageDownloader"
    if self.pid:
      self.pp += "." + str(self.pid)

  def get_images_urls(self, msg_value):
    # Filter 'objects' array based on 'content_type' to get only images urls
    list_urls = []
    if 'objects' in msg_value:
      for obj_pos, obj_val in enumerate(msg_value['objects']):
        if 'content_type' in obj_val and obj_val['content_type'].startswith("image"):
          # prepend prefix so images URLs are actually valid
          image_url = self.url_prefix + obj_val['obj_stored_url']
          # need to keep obj_pos for alignment
          list_urls.append((image_url, obj_pos))
    else:
      print "[{}.get_images_urls: info] Message had not 'objects' fields.".format(self.pp)

    return list_urls

  def build_cdr_msg(self, msg_value, dict_imgs):
    # Edit 'objects' array to add 'img_info', and 'img_sha1' for images
    for url in dict_imgs:
      img = dict_imgs[url]
      tmp_obj = msg_value['objects'][img['obj_pos']]
      tmp_obj['img_info'] = img['img_info']
      tmp_obj['img_sha1'] = img['sha1']
      msg_value['objects'][img['obj_pos']] = tmp_obj
    return json.dumps(msg_value).encode('utf-8')

  def build_image_msg(self, dict_imgs):
    # Build dict output for each image with fields 's3_url', 'sha1', 'img_info' and 'img_buffer'
    img_out_msgs = []
    for url in dict_imgs:
      tmp_dict_out = dict()
      tmp_dict_out['s3_url'] = url
      tmp_dict_out['sha1'] = dict_imgs[url]['sha1']
      tmp_dict_out['img_info'] = dict_imgs[url]['img_info']
      # encode buffer in B64 for JSON dumping
      tmp_dict_out['img_buffer'] = buffer_to_B64(dict_imgs[url]['img_buffer'])
      img_out_msgs.append(json.dumps(tmp_dict_out).encode('utf-8'))
    return img_out_msgs

  def process_one(self, msg):
    from ..imgio.imgio import get_SHA1_img_info_from_buffer, get_buffer_from_URL

    self.print_stats(msg)
    msg_value = json.loads(msg.value)

    # From msg value get list_urls for image objects only
    list_urls = self.get_images_urls(msg_value)

    # Get images data and infos
    dict_imgs = dict()
    # Could we multi-thread that?
    for url, obj_pos in list_urls:
      # process time is by image and not by msg...
      start_process = time.time()
      if self.verbose > 2:
        print_msg = "[{}.process_one: info] Downloading image from: {}"
        print print_msg.format(self.pp, url)
      try:
        img_buffer = get_buffer_from_URL(url)
        if img_buffer:
          sha1, img_type, width, height = get_SHA1_img_info_from_buffer(img_buffer)
          dict_imgs[url] = {'obj_pos': obj_pos, 'img_buffer': img_buffer, 'sha1': sha1,
                            'img_info': {'format': img_type, 'width': width, 'height': height}}
          self.toc_process_ok(start_process)
        else:
          self.toc_process_failed(start_process)
          if self.verbose > 1:
            print_msg = "[{}.process_one: info] Could not download image from: {}"
            print print_msg.format(self.pp, url)
      except Exception as inst:
        self.toc_process_failed(start_process)
        if self.verbose > 0:
          print_msg = "[{}.process_one: error] Could not download image from: {} ({})"
          print print_msg.format(self.pp, url, inst)
          sys.stdout.flush()

    # Push to cdr_out_topic
    self.producer.send(self.cdr_out_topic, self.build_cdr_msg(msg_value, dict_imgs))

    # TODO: we could have all extraction registered here, and not pushing an image if it has been processed by all extractions. But that violates the consumer design of Kafka...
    # Push to images_out_topic
    for img_out_msg in self.build_image_msg(dict_imgs):
      self.producer.send(self.images_out_topic, img_out_msg)


class ThreadedDownloader(threading.Thread):

  def __init__(self, q_in, q_out):
    threading.Thread.__init__(self)
    self.q_in = q_in
    self.q_out = q_out

  def run(self):
    from ..imgio.imgio import get_SHA1_img_info_from_buffer, get_buffer_from_URL

    while self.q_in.empty() == False:
      try:
        # The queue should already have items, no need to block
        url, obj_pos = self.q_in.get(False)
      except:
        continue

      img_buffer = None
      img_info = None
      inst = None
      start_process = time.time()
      try:
        img_buffer = get_buffer_from_URL(url)
        if img_buffer:
          sha1, img_type, width, height = get_SHA1_img_info_from_buffer(img_buffer)
          img_info = (sha1, img_type, width, height)
          end_process = time.time()
        else:
          end_process = time.time()
      except Exception as inst:
        end_process = time.time()

      # Push
      self.q_out.put((url, obj_pos, img_buffer, img_info, start_process, end_process, inst))

      # Mark as done
      self.q_in.task_done()

class KafkaThreadedImageDownloader(KafkaImageDownloader):

  def __init__(self, global_conf_filename, prefix=default_prefix, pid=None):
    super(KafkaThreadedImageDownloader, self).__init__(global_conf_filename, prefix, pid)
    # Get number of threads
    self.nb_threads = self.get_required_param('nb_threads')

  def set_pp(self):
    self.pp = "KafkaThreadedImageDownloader"
    if self.pid:
      self.pp += "." + str(self.pid)


  def process_one(self, msg):
    self.print_stats(msg)
    msg_value = json.loads(msg.value)

    # From msg value get list_urls for image objects only
    list_urls = self.get_images_urls(msg_value)

    # Initialize queues
    from Queue import Queue
    self.q_in = Queue(0)
    self.q_out = Queue(0)
    threads = []

    if list_urls:
      # Fill input queue
      for item in list_urls:
        self.q_in.put(item)

      # Start threads, at most one per image
      for i in range(min(self.nb_threads, len(list_urls))):
        # should read (url, obj_pos) from self.q_in
        # and push (url, obj_pos, buffer, img_info, start_process, end_process) to self.q_out
        thread = ThreadedDownloader(self.q_in, self.q_out)
        thread.start()
        threads.append(thread)

      # Wait for all tasks to be marked as done
      if self.q_in.qsize() > 0:
        self.q_in.join()

    # Get images data and infos
    dict_imgs = dict()
    while not self.q_out.empty():
      url, obj_pos, img_buffer, img_info, start_process, end_process, inst = self.q_out.get()

      if img_buffer is not None and img_info is not None:
        sha1, img_type, width, height = img_info
        dict_imgs[url] = {'obj_pos': obj_pos, 'img_buffer': img_buffer, 'sha1': sha1,
                      'img_info': {'format': img_type, 'width': width, 'height': height}}
        self.toc_process_ok(start_process, end_process)
      else:
        self.toc_process_failed(start_process, end_process)
        if inst is not None:
          if self.verbose > 0:
            print_msg = "[{}.process_one: error] Could not download image from: {} ({})"
            print print_msg.format(self.pp, url, inst)
            sys.stdout.flush()
        else:
          if self.verbose > 1:
            print_msg = "[{}.process_one: info] Could not download image from: {}"
            print print_msg.format(self.pp, url)
            sys.stdout.flush()

    # Push to cdr_out_topic
    self.producer.send(self.cdr_out_topic, self.build_cdr_msg(msg_value, dict_imgs))

    # Push to images_out_topic
    for img_out_msg in self.build_image_msg(dict_imgs):
      self.producer.send(self.images_out_topic, img_out_msg)

    # # Threads are still alive here, is that OK?
    # for th in threads:
    #   if th.isAlive():
    #     print "[{}.process_one: info] Thread {} is still alive.".format(self.pp, th.name)


class KafkaImageDownloaderFromPkl(GenericKafkaProcessor):
  # To push list of images to be processed from a pickle file containing a dictionary
  # {'update_ids': update['update_ids'], 'update_images': out_update_images}
  # with 'out_update_images' being a list of tuples (sha1, url)

  def __init__(self, global_conf_filename, prefix=default_prefix_frompkl):
    # call GenericKafkaProcessor init (and others potentially)
    super(KafkaImageDownloaderFromPkl, self).__init__(global_conf_filename, prefix)
    # any additional initialization needed, like producer specific output logic
    self.images_out_topic = self.get_required_param('producer_cdr_out_topic')
    self.pkl_path = self.get_required_param('pkl_path')

    self.set_pp()

  def set_pp(self):
    self.pp = "KafkaImageDownloaderFromPkl"

  def get_next_img(self):
    import pickle
    update = pickle.load(open(self.pkl_path, 'rb'))
    for sha1, url in update['update_images']:
      yield sha1, url

  def build_image_msg(self, dict_imgs):
    # Build dict ouput for each image with fields 's3_url', 'sha1', 'img_info' and 'img_buffer'
    img_out_msgs = []
    for url in dict_imgs:
      tmp_dict_out = dict()
      tmp_dict_out['s3_url'] = url
      tmp_dict_out['sha1'] = dict_imgs[url]['sha1']
      tmp_dict_out['img_info'] = dict_imgs[url]['img_info']
      # encode buffer in B64?
      tmp_dict_out['img_buffer'] = buffer_to_B64(dict_imgs[url]['img_buffer'])
      img_out_msgs.append(json.dumps(tmp_dict_out).encode('utf-8'))
    return img_out_msgs

  def process(self):
    from ..imgio.imgio import get_SHA1_img_info_from_buffer, get_buffer_from_URL

    # Get images data and infos
    for sha1, url in self.get_next_img():

      if (self.process_count + self.process_failed) % self.display_count == 0:
        avg_process_time = self.process_time / max(1, self.process_count + self.process_failed)
        print_msg = "[%s] dl count: %d, failed: %d, time: %f"
        print print_msg % (self.pp, self.process_count, self.process_failed, avg_process_time)

      dict_imgs = dict()
      # Could we multi-thread that?
      start_process = time.time()
      if self.verbose > 2:
        print_msg = "[{}.process_one: info] Downloading image from: {}"
        print print_msg.format(self.pp, url)
      try:
        img_buffer = get_buffer_from_URL(url)
        if img_buffer:
          sha1, img_type, width, height = get_SHA1_img_info_from_buffer(img_buffer)
          dict_imgs[url] = {'img_buffer': img_buffer, 'sha1': sha1,
                            'img_info': {'format': img_type, 'width': width, 'height': height}}
          self.toc_process_ok(start_process)
        else:
          self.toc_process_failed(start_process)
          if self.verbose > 1:
            print_msg = "[{}.process_one: info] Could not download image from: {}"
            print print_msg.format(self.pp, url)
      except Exception as inst:
        self.toc_process_failed(start_process)
        if self.verbose > 0:
          print_msg = "[{}.process_one: error] Could not download image from: {} ({})"
          print print_msg.format(self.pp, url, inst)

      # Push to images_out_topic
      for img_out_msg in self.build_image_msg(dict_imgs):
        self.producer.send(self.images_out_topic, img_out_msg)


class DaemonKafkaImageDownloader(multiprocessing.Process):
  daemon = True

  def __init__(self, conf, prefix=default_prefix):
    super(DaemonKafkaImageDownloader, self).__init__()
    self.conf = conf
    self.prefix = prefix

  def run(self):
    while True:
      try:
        print "Starting worker KafkaImageDownloader.{}".format(self.pid)
        kp = KafkaImageDownloader(self.conf, prefix=self.prefix, pid=self.pid)
        for msg in kp.consumer:
          kp.process_one(msg)
      except Exception as inst:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print "KafkaImageDownloader.{} died (In {}:{}, {}:{})".format(self.pid, fname, exc_tb.tb_lineno, type(inst), inst)
      time.sleep(10)


class DaemonKafkaThreadedImageDownloader(multiprocessing.Process):
  daemon = True

  def __init__(self, conf, prefix=default_prefix):
    super(DaemonKafkaThreadedImageDownloader, self).__init__()
    self.conf = conf
    self.prefix = prefix

  def run(self):
    while True:
      try:
        print "Starting worker KafkaThreadedImageDownloader.{}".format(self.pid)
        kp = KafkaThreadedImageDownloader(self.conf, prefix=self.prefix, pid=self.pid)
        for msg in kp.consumer:
          kp.process_one(msg)
      except Exception as inst:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print "KafkaThreadedImageDownloader.{} died (In {}:{}, {}:{})".format(self.pid, fname, exc_tb.tb_lineno, type(inst), inst)
      time.sleep(10)