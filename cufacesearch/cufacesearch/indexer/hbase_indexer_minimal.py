import sys
import time
import happybase
from datetime import datetime
from ..common.conf_reader import ConfReader
from ..common.error import full_trace_error
from ..common import update_prefix, column_list_sha1s


TTransportException = happybase._thriftpy.transport.TTransportException
TException = happybase._thriftpy.thrift.TException
max_errors = 3
# reading a lot of data from HBase at once can be unstable
batch_size = 100
extr_str_processed = "processed"
update_str_processed = "processed"
update_str_started = "started"
update_str_created = "created"
update_str_completed = "completed"
info_column_family = "info"
update_completed_column = info_column_family+":"+update_str_completed
img_buffer_column = info_column_family+":img_buffer"
img_URL_column = info_column_family+":s3_url"
img_path_column = info_column_family+":img_path"
extraction_column_family = "ext"
default_prefix = "HBI_"

class HBaseIndexerMinimal(ConfReader):

  def __init__(self, global_conf_in, prefix=default_prefix):
    self.last_refresh = datetime.now()
    self.transport_type = 'buffered'  # this is happybase default
    # self.transport_type = 'framed'
    self.timeout = 4
    # to store count of batches of updates pushed
    self.dict_up = dict()
    self.batch_update_size = 1000
    # could be set in parameters
    self.column_list_sha1s = column_list_sha1s
    super(HBaseIndexerMinimal, self).__init__(global_conf_in, prefix)

  def set_pp(self):
    self.pp = "HBaseIndexerMinimal"

  def read_conf(self):
    """ Reads configuration parameters.

    Will read parameters 'host', 'table_sha1infos'...
    from self.global_conf.
    """
    super(HBaseIndexerMinimal, self).read_conf()
    # HBase conf
    self.hbase_host = str(self.get_required_param('host'))
    self.table_sha1infos_name = str(self.get_required_param('table_sha1infos'))
    self.table_updateinfos_name = str(self.get_param('table_updateinfos'))
    if self.verbose > 0:
      print_msg = "[{}.read_conf: info] HBase tables name: {} (sha1infos), {} (updateinfos)"
      print print_msg.format(self.pp, self.table_sha1infos_name, self.table_updateinfos_name)
    self.nb_threads = 1
    param_nb_threads = self.get_param('pool_thread')
    if param_nb_threads:
      self.nb_threads = param_nb_threads
    batch_update_size = self.get_param('batch_update_size')
    if batch_update_size:
      if int(batch_update_size) > 0:
        self.batch_update_size = int(batch_update_size)
    from thriftpy.transport import TTransportException
    self.pool = None
    try:
      # The timeout as parameter seems to cause issues?...
      self.pool = happybase.ConnectionPool(timeout=60000, size=self.nb_threads, host=self.hbase_host,
                                           transport=self.transport_type)
      #self.pool = happybase.ConnectionPool(size=self.nb_threads, host=self.hbase_host, transport=self.transport_type)
    except TTransportException as inst:
      print_msg = "[{}.read_conf: error] Could not initalize connection to HBase. Are you connected to the VPN?"
      print print_msg.format(self.pp)
      #raise inst

      # # Extractions configuration (TO BE IMPLEMENTED)
      # self.extractions_types = self.get_param('extractions_types')
      # self.extractions_columns = self.get_param('extractions_columns')
      # if len(self.extractions_columns) != len(self.extractions_types):
      #     raise ValueError("[HBaseIndexerMinimal.read_conf: error] Dimensions mismatch {} vs. {} for extractions_columns vs. extractions_types".format(len(self.extractions_columns),len(self.extractions_types)))

  def refresh_hbase_conn(self, calling_function, sleep_time=0):
    # this can take up to 4 seconds...
    try:
      start_refresh = time.time()
      dt_iso = datetime.utcnow().isoformat()
      print_msg = "[{}.{}: {}] caught timeout error or TTransportException. Trying to refresh connection pool."
      print print_msg.format(self.pp, calling_function, dt_iso)
      sys.stdout.flush()
      time.sleep(sleep_time)
      # This can hang for a long time?
      # Should we add timeout (in ms: http://happybase.readthedocs.io/en/latest/api.html#connection)?
      #self.pool = happybase.ConnectionPool(size=self.nb_threads, host=self.hbase_host, transport=self.transport_type)
      self.pool = happybase.ConnectionPool(timeout=60000, size=self.nb_threads, host=self.hbase_host, transport=self.transport_type)
      print_msg = "[{}.refresh_hbase_conn: log] Refreshed connection pool in {}s."
      print print_msg.format(self.pp, time.time()-start_refresh)
      sys.stdout.flush()
    except TTransportException as inst:
      print_msg = "[{}.read_conf: error] Could not initalize connection to HBase ({}). Are you connected to the VPN?"
      print print_msg.format(self.pp, inst)
      sys.stdout.flush()
      #raise inst

  def check_errors(self, previous_err, function_name, inst=None):
    if previous_err >= max_errors:
      err_msg = "[HBaseIndexerMinimal: error] function {} reached maximum number of error {}. Error {} was: {}"
      raise Exception(err_msg.format(function_name, max_errors, type(inst), inst))
    return None

  def get_check_column(self, extraction):
    return extraction_column_family+":"+"_".join([extraction, extr_str_processed])


  def get_create_table(self, table_name, conn=None, families={'info': dict()}):
    try:
      if conn is None:
        from happybase.connection import Connection
        conn = Connection(self.hbase_host)
      try:
        # what exception would be raised if table does not exist, actually none.
        # need to try to access families to get error
        table = conn.table(table_name)
        # this would fail if table does not exist
        _ = table.families()
        return table
      except Exception as inst:
        # TODO: act differently based on error type (connection issue or actually table missing)
        if type(inst) == TTransportException:
          raise inst
        else:
          print "[{}.get_create_table: info] table {} does not exist (yet): {}{}".format(self.pp, table_name, type(inst), inst)
          conn.create_table(table_name, families)
          table = conn.table(table_name)
          print "[{}.get_create_table: info] created table {}".format(self.pp, table_name)
          return table
    except Exception as inst:
      # May fail if families in dictionary do not match those of an existing table, or because of connection issues?
      # Should we raise it up?
      #pass
      raise inst

  # use 'row_prefix' http://happybase.readthedocs.io/en/latest/api.html?highlight=scan#happybase.Table.scan?
  def scan_with_prefix(self, table_name, row_prefix=None, columns=None, maxrows=10, previous_err=0, inst=None):
    self.check_errors(previous_err, "scan_with_prefix", inst)
    try:
      with self.pool.connection(timeout=self.timeout) as connection:
        hbase_table = connection.table(table_name)
        # scan table for rows with row_prefix, and accumulate in rows the information of the columns that are needed
        rows = []
        for one_row in hbase_table.scan(row_prefix=row_prefix, columns=columns, batch_size=10):
          # print "one_row:",one_row
          rows.extend((one_row,))
          if len(rows) >= maxrows:
            return rows
          if self.verbose:
            print("[scan_with_prefix: log] got {} rows.".format(len(rows)))
            sys.stdout.flush()
        return rows
    except Exception as inst:
      print "scan_with_prefix", inst
      # try to force longer sleep time...
      self.refresh_hbase_conn("scan_with_prefix", sleep_time=4 * previous_err)
      return self.scan_with_prefix(table_name, row_prefix=row_prefix, columns=columns, maxrows=maxrows,
                                previous_err=previous_err + 1, inst=inst)

  def scan_from_row(self, table_name, row_start=None, columns=None, maxrows=10, previous_err=0, inst=None):
    self.check_errors(previous_err, "scan_from_row", inst)
    try:
      with self.pool.connection(timeout=self.timeout) as connection:
        hbase_table = connection.table(table_name)
        # scan table from row_start, and accumulate in rows the information of the columns that are needed
        rows = []
        #for one_row in hbase_table.scan(row_start=row_start, columns=columns, batch_size=2):
        for one_row in hbase_table.scan(row_start=row_start, columns=columns, batch_size=maxrows):
          #print "one_row:",one_row[0]
          rows.extend((one_row,))
          if len(rows) >= maxrows:
            return rows
          if self.verbose:
            print("[scan_from_row: log] got {} rows.".format(len(rows)))
            sys.stdout.flush()
        return rows
    except Exception as inst:
      print "scan_from_row", inst
      # try to force longer sleep time...
      self.refresh_hbase_conn("scan_from_row", sleep_time=4*previous_err)
      return self.scan_from_row(table_name, row_start=row_start, columns=columns, maxrows=maxrows,
                                previous_err=previous_err + 1, inst=inst)

  # def get_updates_from_date(self, start_date, extr_type="", maxrows=10, previous_err=0, inst=None):
  #   # start_date should be in format YYYY-MM-DD(_XX)
  #   rows = None
  #   self.check_errors(previous_err, "get_updates_from_date", inst)
  #   # build row_start as index_update_YYYY-MM-DD
  #   row_start = update_prefix + extr_type + "_" + start_date
  #   try:
  #     # Should we add an option to exclude row_start?
  #     rows = self.scan_from_row(self.table_updateinfos_name, row_start=row_start, maxrows=maxrows)
  #   except Exception as inst: # try to catch any exception
  #     print "[get_updates_from_date: error] {}".format(inst)
  #     self.refresh_hbase_conn("get_updates_from_date")
  #     return self.get_updates_from_date(start_date, extr_type=extr_type, maxrows=maxrows, previous_err=previous_err+1,
  #                                       inst=inst)
  #   return rows

  def get_updates_from_date(self, start_date, extr_type="", maxrows=100, previous_err=0, inst=None):
    # start_date should be in format YYYY-MM-DD(_XX)
    rows = None
    self.check_errors(previous_err, "get_updates_from_date", inst)
    # build row_start as index_update_YYYY-MM-DD
    row_start = update_prefix + extr_type + "_" + start_date
    try:
      while True:
        rows = self.scan_from_row(self.table_updateinfos_name, row_start=row_start, maxrows=maxrows)
        if rows:
          if extr_type:
            # Filter out updates of other extractions type.
            out_rows = []
            for row in rows:
              if extr_type in row[0]:
                out_rows.append((row[0],row[1]))
          else:
            out_rows = rows
          if out_rows:
            yield out_rows
          # add '~' to exclude last row from next batch
          row_start = rows[-1][0]+'~'
        else:
          #print "[{}.get_updates_from_date: log] 'rows' was None.".format(self.pp)
          break
    except Exception as inst: # try to catch any exception
      print "[{}.get_updates_from_date: error] {}".format(self.pp, inst)
      self.refresh_hbase_conn("get_updates_from_date")
      try:
        for out_rows in self.get_updates_from_date(start_date, extr_type=extr_type, maxrows=maxrows, previous_err=previous_err+1,
                                        inst=inst):
          yield out_rows
      except Exception as inst:
        raise inst


  def get_unprocessed_updates_from_date(self, start_date, extr_type="", maxrows=100, previous_err=0, inst=None):
    # start_date should be in format YYYY-MM-DD(_XX)
    rows = None
    self.check_errors(previous_err, "get_unprocessed_updates_from_date", inst)
    # build row_start as index_update_YYYY-MM-DD
    row_start = update_prefix + "_" + extr_type + "_" + start_date
    #print row_start
    last_row = row_start
    try:
      tmp_rows = self.scan_from_row(self.table_updateinfos_name, row_start=row_start, columns=None,
                                    maxrows=maxrows, previous_err=0, inst=None)
      if tmp_rows:
        for row_id, row_val in tmp_rows:
          last_row = row_id
          #if "info:"+update_str_processed not in row_val and "info:"+update_str_started not in row_val:
          if "info:" + update_str_processed not in row_val:
            #print "row:",row_id
            if rows is None:
              rows = [(row_id, row_val)]
            else:
              rows.append((row_id, row_val))
      if rows:
        if tmp_rows is None or len(tmp_rows) < maxrows:
          # Looks like we really have nothing to process...
          return rows
        else:
          # Explore further
          next_start_date = '_'.join(last_row.split('_')[-2:])
          # Multiply maxrows to avoid maximum recursion depth issue...
          return self.get_unprocessed_updates_from_date(next_start_date, extr_type=extr_type,
                                                        maxrows=maxrows)
    except Exception as inst: # try to catch any exception
      full_trace_error("[get_unprocessed_updates_from_date: error] {}".format(inst))
      self.refresh_hbase_conn("get_unprocessed_updates_from_date", sleep_time=4*previous_err)
      return self.get_unprocessed_updates_from_date(start_date, extr_type=extr_type, maxrows=maxrows,
                                                    previous_err=previous_err+1, inst=inst)
    return rows

  def get_missing_extr_updates_from_date(self, start_date, extr_type="", maxrows=100, previous_err=0, inst=None):
    # start_date should be in format YYYY-MM-DD(_XX)
    rows = None
    self.check_errors(previous_err, "get_missing_extr_updates_from_date", inst)
    # build row_start as index_update_YYYY-MM-DD
    row_start = update_prefix + extr_type + "_" + start_date
    next_start_date = start_date

    try:
      while True:
        rows = self.scan_from_row(self.table_updateinfos_name, row_start=row_start, maxrows=maxrows)
        if rows:
          out_rows = []
          if extr_type:
            # Filter out updates of other extractions type.
            for row in rows:
              next_start_date = '_'.join(row[0].split('_')[-2:])
              if extr_type in row[0]:
                if update_completed_column in row[1]:
                  # Update has been marked as all extractions being performed
                  continue
                if column_list_sha1s in row[1]:
                  missing_extr_sha1s = self.get_missing_extr_sha1s(row[1][column_list_sha1s].split(','), extr_type)
                  if missing_extr_sha1s:
                    out_row_val = dict()
                    out_row_val[column_list_sha1s] = ','.join(missing_extr_sha1s)
                    if out_rows:
                      out_rows.append((row[0], out_row_val))
                    else:
                      out_rows = [(row[0], out_row_val)]
                  else:
                    # We should mark as completed here
                    update_completed_dict = {row[0]: {info_column_family + ':' + update_str_completed: str(1)}}
                    self.push_dict_rows(dict_rows=update_completed_dict,
                                        table_name=self.table_updateinfos_name)
                else:
                  print "[get_missing_extr_updates_from_date: warning] update {} has no list of image.".format(row[0])
          if out_rows:
            yield out_rows
            # add '~' to exclude last row from next batch
          row_start = rows[-1][0]+'~'
        else:
          print "[get_missing_extr_updates_from_date: log] No update with unprocessed images found."
          break
    except Exception as inst: # try to catch any exception
      print "[get_missing_extr_updates_from_date: error] {}".format(inst)
      self.refresh_hbase_conn("get_missing_extr_updates_from_date")
      yield self.get_missing_extr_updates_from_date(next_start_date, extr_type=extr_type, maxrows=maxrows, previous_err=previous_err+1,
                                        inst=inst)

  def get_today_string(self):
    return datetime.today().strftime('%Y-%m-%d')

  def get_next_update_id(self, today=None, extr_type=""):
    # get today's date as in format YYYY-MM-DD
    if today is None:
      today = self.get_today_string()
    if today not in self.dict_up:
      self.dict_up = dict()
      self.dict_up[today] = 0
    else:
      self.dict_up[today] += 1
    # add the extraction type, as different extraction may build different batches depending
    # when they started to process the images
    update_id = update_prefix + extr_type + "_" + today + "_" + str(self.dict_up[today])
    return update_id, today

  # deprecated
  # def get_batch_update(self, list_sha1s):
  #   l = len(list_sha1s)
  #   for ndx in range(0, l, self.batch_update_size):
  #     yield list_sha1s[ndx:min(ndx + self.batch_update_size, l)]

  # deprecated
  # def push_list_updates(self, list_sha1s, previous_err=0, inst=None):
  #   self.check_errors(previous_err, "push_list_updates", inst)
  #   today = None
  #   dict_updates = dict()
  #   # Build batches of self.batch_update_size of images updates
  #   # NB: this batching is redundant with what is done in 'full_image_updater_kafka_to_hbase'
  #   # but ensures batches have the right size even if called from somewhere else...
  #   for batch_list_sha1s in self.get_batch_update(list_sha1s):
  #     update_id, today = self.get_next_update_id(today)
  #     dict_updates[update_id] = {self.column_list_sha1s: ','.join(batch_list_sha1s)}
  #   # Push them
  #   self.push_dict_rows(dict_updates, self.table_updateinfos_name)

  # Deprecated
  def push_list_updates(self, list_sha1s, update_id):
    """ Push the 'update_id' composed of the images in 'list_sha1s' to 'table_updateinfos_name'.

    :param list_sha1s: list of the images SHA1
    :param update_id: update identifier
    """
    # Build update dictionary
    dict_updates = dict()
    dict_updates[update_id] = {self.column_list_sha1s: ','.join(list_sha1s)}
    # Push it
    self.push_dict_rows(dict_updates, self.table_updateinfos_name)

  def push_dict_rows(self, dict_rows, table_name, families=None, previous_err=0, inst=None):
    """ Push a dictionary to the HBase 'table_name' assuming keys are the row keys and each entry is a valid dictionary
    containing the column names and values.

    :param dict_rows: input dictionary to be pushed.
    :param table_name: name of the HBase table where to push the data.
    :param families: all families of the table (if we need to create the table)
    :param previous_err: number of previous errors caught
    :param inst: previous error instance caught
    :return: None
    """

    # Can get an IllegalArgument(message='java.lang.IllegalArgumentException: KeyValue size too large
    self.check_errors(previous_err, "push_dict_rows", inst)
    hbase_table = None
    retries = 0
    batch_size = 10
    if previous_err > 0:
      batch_size = 1
    try:
      # Use connection pool. Seems to fail when pool was initialized a long time ago...
      with self.pool.connection(timeout=self.timeout) as connection:
        if families:
          hbase_table = self.get_create_table(table_name, families=families, conn=connection)
        else:
          hbase_table = self.get_create_table(table_name, conn=connection)

        if hbase_table is None:
          raise ValueError("Could not initialize hbase_table")

        # Sometimes get KeyValue size too large when inserting processed images...
        # Usually happens for GIF images
        b = hbase_table.batch(batch_size=batch_size) # should we have a bigger batch size?
        # Assume dict_rows[k] is a dictionary ready to be pushed to HBase...
        for k in dict_rows:
          if previous_err > 1:
            tmp_dict_row = dict_rows[k]
            row_size = sys.getsizeof(tmp_dict_row)
            for kk in tmp_dict_row:
              row_size += sys.getsizeof(tmp_dict_row[kk])
            if row_size > 2097152: # print warning if size is bigger than 2MB?
              print "[{}: warning] Row {} size seems to be: {}. Keys are: {}".format(self.pp, k, row_size, tmp_dict_row.keys())
              sys.stdout.flush()
              # Try to discard buffer to avoid 'KeyValue size too large'
              if img_buffer_column in tmp_dict_row:
                del tmp_dict_row[img_buffer_column]
            b.put(k, tmp_dict_row)
          else:
            b.put(k, dict_rows[k])
        b.send()

      # # Let get_create_table set up the connection, seems to fail too
      # if families:
      #   hbase_table = self.get_create_table(table_name, families=families)
      # else:
      #  hbase_table = self.get_create_table(table_name)
      # b = hbase_table.batch(batch_size=10) # should we have a bigger batch size?
      # # Assume dict_rows[k] is a dictionary ready to be pushed to HBase...
      # for k in dict_rows:
      #   b.put(k, dict_rows[k])
      # b.send()
    except Exception as inst: # try to catch any exception
      #print "[push_dict_rows: error] {}".format(inst)
      if previous_err+1 == max_errors:
        print "[push_dict_rows: log] dict_rows keys: {}".format(dict_rows.keys())
      self.refresh_hbase_conn("push_dict_rows", sleep_time=4*previous_err)
      return self.push_dict_rows(dict_rows, table_name, families=families, previous_err=previous_err+1, inst=inst)

  def get_rows_by_batch(self, list_queries, table_name, families=None, columns=None, previous_err=0, inst=None):
    self.check_errors(previous_err, "get_rows_by_batch", inst)
    try:
      with self.pool.connection(timeout=self.timeout) as connection:
        #hbase_table = connection.table(table_name)
        if families:
          hbase_table = self.get_create_table(table_name, families=families, conn=connection)
        else:
          hbase_table = self.get_create_table(table_name, conn=connection)
        if hbase_table:
          # slice list_queries in batches of batch_size to query
          rows = []
          nb_batch = 0
          for batch_start in range(0,len(list_queries), batch_size):
            batch_list_queries = list_queries[batch_start:min(batch_start+batch_size,len(list_queries))]
            rows.extend(hbase_table.rows(batch_list_queries, columns=columns))
            nb_batch += 1
          if self.verbose:
            print("[get_rows_by_batch: log] got {} rows using {} batches.".format(len(rows), nb_batch))
          return rows
        else:
          err_msg = "[get_rows_by_batch: error] could not get table: {} (families: {})"
          raise ValueError(err_msg.format(table_name, families))
    except Exception as inst:
      if type(inst) == ValueError:
        raise inst
      # try to force longer sleep time if error repeats...
      self.refresh_hbase_conn("get_rows_by_batch", sleep_time=previous_err)
      return self.get_rows_by_batch(list_queries, table_name, families=families, columns=columns,
                                    previous_err=previous_err+1, inst=inst)

  def get_columns_from_sha1_rows(self, list_sha1s, columns, families=None, previous_err=0, inst=None):
    rows = None
    self.check_errors(previous_err, "get_columns_from_sha1_rows", inst)
    if list_sha1s:
      try:
        #print self.table_sha1infos_name
        rows = self.get_rows_by_batch(list_sha1s, self.table_sha1infos_name, families=families, columns=columns)
      except Exception as inst: # try to catch any exception
        print "[get_columns_from_sha1_rows: error] {}".format(inst)
        self.refresh_hbase_conn("get_columns_from_sha1_rows")
        return self.get_columns_from_sha1_rows(list_sha1s, columns, families=families, previous_err=previous_err+1, inst=inst)
    return rows


  def get_features_from_sha1s(self, list_sha1s, extr_type, feat_type_decode=None):
    from ..featurizer.featsio import featB64decode

    # Cannot use column filters here...
    has_detection = False
    if "_".join(extr_type.split("_")[-2:]) != "full_image":
      has_detection = True
    # sbpycaffe is saved as np.float32 while dlib face features are np.float64
    if feat_type_decode is None:
      feat_type_decode = extr_type.split("_")[0]
    # We need to get all extractions and parse them for matches with extr_type...
    # We could also read image infos if we need to filter things out based on format and/or image size

    #print list_sha1s
    rows = self.get_columns_from_sha1_rows(list_sha1s, columns=[extraction_column_family])
    samples_id = []
    feats = []
    for row in rows:
      for k in row[1]:
        #print k
        if k.startswith(extraction_column_family+":"+extr_type) and not k.endswith("_updateid") and not k.endswith(extr_str_processed):
          # Get sample id
          if not has_detection:
            sid = str(row[0])
          else:
            # parse to get id, sha1 + detection_box
            sid = str(row[0])+"_"+"_".join(k.split("_")[4:8])
          # Get feature
          feat = featB64decode(row[1][k], feat_type_decode)
          # Add sample id and feature
          #print sid, feat.shape
          samples_id.append(sid)
          feats.append(feat)
    if self.verbose > 0:
      print "[{}: info] Got {} rows and {} features.".format(self.pp, len(rows), len(samples_id))
    return samples_id, feats

  def get_missing_extr_sha1s(self, list_sha1s, extr_type):
    rows = self.get_columns_from_sha1_rows(list_sha1s, columns=[extraction_column_family])
    sha1s_w_extr = set()
    for row in rows:
      for k in row[1]:
        if k.startswith(extraction_column_family+":"+extr_type) and k.endswith(extr_str_processed):
          sha1s_w_extr.add(str(row[0]))
    #print "Found {} sha1s with extractions".format(len(sha1s_w_extr))
    return list(set(list_sha1s) - sha1s_w_extr)