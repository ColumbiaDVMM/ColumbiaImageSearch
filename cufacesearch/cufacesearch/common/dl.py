import os
import sys
import time
from six.moves import urllib

def reporthook(count, block_size, total_size):
  """
  From http://blog.moleculea.com/2012/10/04/urlretrieve-progres-indicator/
  """
  global start_time, last_time
  if count == 0:
    start_time = time.time()
    return
  last_time = time.time()
  duration = (time.time() - start_time) or 0.01
  if last_time - start_time > 1:
    progress_size = int(count * block_size)
    speed = int(progress_size / (1024 * duration))
    percent = int(count * block_size * 100 / total_size)
    # Seems to get stuck after 10 seconds of downloading...
    #sys.stdout.write("\r...%d%%, %d MB, %d KB/s, %d seconds passed" %
    sys.stdout.write("\n%d%%, %d MB, %d KB/s, %d seconds passed" %
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

def mkpath(outpath):
  pos_slash = [pos for pos, c in enumerate(outpath) if c == "/"]
  for pos in pos_slash:
    try:
      os.mkdir(outpath[:pos])
    except:
      pass

def untar_file(fname, outpath):
  import tarfile
  if fname.endswith("tar.gz") or fname.endswith("tgz"):
    tar = tarfile.open(fname, "r:gz")
    tar.extractall(path=outpath)
    tar.close()
  elif (fname.endswith("tar")):
    tar = tarfile.open(fname, "r:")
    tar.extractall(path=outpath)
    tar.close()

def fixurl(url):
  # Inspired from https://stackoverflow.com/a/804380 but using requests
  from requests.utils import urlparse, urlunparse, quote, unquote

  # turn string into unicode
  if not isinstance(url, unicode):
    url = url.decode('utf8')

  # parse it
  parsed = urlparse(url)

  # divide the netloc further
  userpass, at, hostport = parsed.netloc.rpartition('@')
  user, colon1, pass_ = userpass.partition(':')
  host, colon2, port = hostport.partition(':')

  # encode each component
  scheme = parsed.scheme.encode('utf8')
  user = quote(user.encode('utf8'))
  colon1 = colon1.encode('utf8')
  pass_ = quote(pass_.encode('utf8'))
  at = at.encode('utf8')
  host = host.encode('idna')
  colon2 = colon2.encode('utf8')
  port = port.encode('utf8')
  path = '/'.join(  # could be encoded slashes!
    quote(unquote(pce).encode('utf8'), '')
    for pce in parsed.path.split('/')
  )
  query = quote(unquote(parsed.query).encode('utf8'), '=&?/')
  fragment = quote(unquote(parsed.fragment).encode('utf8'))

  # put it back together
  netloc = ''.join((user, colon1, pass_, at, host, colon2, port))
  #urlunparse((scheme, netloc, path, params, query, fragment))
  params = ''
  return urlunparse((scheme, netloc, path, params, query, fragment))

