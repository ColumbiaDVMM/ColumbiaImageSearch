import os
import sys
import time
from six.moves import urllib

def reporthook(count, block_size, total_size):
  """
  From http://blog.moleculea.com/2012/10/04/urlretrieve-progres-indicator/
  """
  global start_time
  if count == 0:
    start_time = time.time()
    return
  duration = (time.time() - start_time) or 0.01
  progress_size = int(count * block_size)
  speed = int(progress_size / (1024 * duration))
  percent = int(count * block_size * 100 / total_size)
  sys.stdout.write("\r...%d%%, %d MB, %d KB/s, %d seconds passed" %
                   (percent, progress_size / (1024 * 1024), speed, duration))
  sys.stdout.flush()

def download_file(url, local_path):
  """ Download file from 'url' to the directory of 'local_path'

  :param url: url of model to download
  :param local_path: final local path of model
  """
  print "Downloading file from: {}".format(url)
  out_dir = os.path.dirname(local_path)
  try:
    os.makedirs(out_dir)
  except:
    pass
  urllib.request.urlretrieve(url, local_path, reporthook)