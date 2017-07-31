# Look for images that have been indexed after a given date to catch up in terms of face detection.
# Prepare data for processing with spark workflow

from argparse import ArgumentParser
import os
import sys
import json
import time
import pickle
import happybase


default_conf = "../conf/global_conf_get_new_face_hbase.json"


def get_update_infos(opts):
  # Check if results were already computed
  if os.path.exists(opts.update_file):
    start_load = time.time()
    print "Loading update from "+opts.update_file
    out = pickle.load(open(opts.update_file, 'rb'))
    print "Loaded in {}s.".format(time.time() - start_load)
  else:
    # Load config infos
    conf = json.load(open(opts.conf, 'rt'))
    table_name = conf['HBFI_table_updateinfos']
    host = conf['HBFI_host']

    # Setup HBase connection
    connection = happybase.Connection(host)
    hbase_table = connection.table(table_name)

    # Prepare scan
    row_start = opts.row_update_prefix + opts.start_date
    columns = opts.columns_update
    update_ids = []
    update_sha1s = []

    # Perform scan
    for one_row in hbase_table.scan(row_start=row_start, columns=columns, batch_size=5):
      split_index_id = one_row[0].split('_')
      if len(split_index_id) == 4:
        split_date = split_index_id[2].split('-')
        if len(split_date) == 3:
          print one_row[0]
          update_ids.extend((one_row[0],))
          update_sha1s.extend(one_row[1][columns[0]].split(','))
        else:
          # Update previously were formatted as index_update_ts
          print "Reached old format of updates. Breaking"
          break
      if opts.verbose:
        print("[get_update_infos: log] got {} updates containing {} new images.".format(len(update_ids), len(update_sha1s)))
        sys.stdout.flush()

    # save results
    print "Saving update to " + opts.update_file
    out = {'update_ids': update_ids, 'update_sha1s': update_sha1s}
    pickle.dump(out, open(opts.update_file, 'wb'))

  return out

def chunks(l, n):
  """Yield successive n-sized chunks from l."""
  for i in xrange(0, len(l), n):
    yield l[i:i + n]

def get_images_urls(update, opts):
  # Check if results were already computed
  if os.path.exists(opts.out_file):
    print "Out file already computed at " + opts.out_file
    return None
  else:
    # Load config infos
    conf = json.load(open(opts.conf, 'rt'))
    table_name = conf['HBFI_table_sha1infos']
    host = conf['HBFI_host']

    # Setup HBase connection
    connection = happybase.Connection(host)
    hbase_table = connection.table(table_name)

    # Prepare
    columns = opts.columns_url
    columns.extend(opts.columns_face)
    print columns
    out_update_images = []

    # Perform queries
    # out['update_sha1s'] can be several millions...
    batch_size = 1000
    for batch_rows in chunks(update['update_sha1s'],batch_size):
      rows = hbase_table.rows(rows=batch_rows, columns=columns)
      for row in rows:
        # Did we just got the URL back?
        if len(row[1].keys()) == 1:
          out_update_images.append((row[0],row[1][opts.columns_url[0]]))
        else:
          # Otherwise, face detection was already run...
          if opts.verbose > 1:
            print "Face detection already run for image {} with columns {}".format(row[0], row[1])

      if opts.verbose:
        print("[get_images_urls: log] got {} urls of new images.".format(len(out_update_images)))

    # save results
    print "Saving out to " + opts.out_file
    out = {'update_ids': update['update_ids'],  'update_images': out_update_images}
    pickle.dump(out, open(opts.out_file, 'wb'))

  return out


if __name__ == "__main__":

  # Get options
  parser = ArgumentParser()
  parser.add_argument("-s", "--start_date", dest="start_date", required=True)
  parser.add_argument("-c", "--conf", dest="conf", default=default_conf)
  parser.add_argument("-v", "--verbose", dest="verbose", type=int, default=1)
  parser.add_argument("-o", "--out_file", dest="out_file", type=str, default="update_wurl.pkl")
  parser.add_argument("-u", "--update_file", dest="update_file", type=str, default="update.pkl")
  parser.add_argument("--row_update_prefix", dest="row_update_prefix", default="index_update_")
  parser.add_argument("--columns_update", dest="columns_update", default=["info:list_sha1s"])
  parser.add_argument("--columns_url", dest="columns_url", default=["info:s3_url"])
  parser.add_argument("--columns_face", dest="columns_face", default=["face"])

  opts = parser.parse_args()

  # Get update infos
  update = get_update_infos(opts)

  # Get images
  out = get_images_urls(update, opts)