from __future__ import print_function

import sys
import time
from datetime import datetime

import happybase
from ..common.conf_reader import ConfReader
from ..common.error import full_trace_error
from ..common import update_prefix, column_list_sha1s

from thriftpy.transport import TTransportException
#TTransportException = happybase._thriftpy.transport.TTransportException
#TException = happybase._thriftpy.thrift.TException

update_str_processed = "processed"
update_str_started = "started"
UPDATE_STR_CREATED = "created"
update_str_completed = "completed"

# Before transition:
EXTR_STR_PROCESSED = "processed"
UPDATE_INFOCF = "info"
IMG_INFOCF = "info"
IMG_BUFFCF = "info"
IMG_BUFFCNAME = "img_buffer"
info_column_family = "info" # deprecated, as we need to distinguish img/update info column family
#extraction_column_family = "ext"
EXTR_CF = "ext"
# Should we just define column names here?
# Where are these variables used? Can they handle being dynamically set through conf files?...
img_buffer_column = IMG_INFOCF + ":" + IMG_BUFFCNAME
img_URL_column = IMG_INFOCF + ":s3_url"
img_path_column = IMG_INFOCF + ":img_path"

default_prefix = "HBI_"

# Maximum number of retries before actually raising error
MAX_ERRORS = 3
# Reading a lot of data from HBase at once can be unstable
READ_BATCH_SIZE = 100
# Maximum number of rows when scanning (could be renamed to be more explicit)
MAX_ROWS = 500
# Maximum size of one row to be saved to HBase (could be dependent on HBase setup)
MAX_ROW_SIZE = 2097152
#UPDATE_BATCH_SIZE = 2048
UPDATE_BATCH_SIZE = 1000

# After transition
# TODO: define these values in a way that could work with merged tables with Uncharted
# Put everything in "data" column family and img:img for buffer but we SHOULD NOT write to it
#  data vs. info, double check the consequences downstream.
# should we have an "info_column_family" and an "image_info_column_family"
# Uncharted format data:location for s3_url but we should not overwrite...
# we could use it as backup for getting s3_url?
# Could all these be specified dynamically, e.g. in conf file?
# img_info_column_family = "data"
# img_buffer_column_family = "img"
# update_info_column_family = "info"
# extraction_column_family = "data"
# update_completed_column = update_info_column_family+":"+update_str_completed
# img_buffer_column = img_buffer_column_family+":img"
# img_URL_column = img_info_column_family+":s3_url"
# img_backup_URL_column = img_info_column_family+":location"
# img_path_column = img_info_column_family+":img_path"
# default_prefix = "HBI_"
# MAX_ROWS = 500


class HBaseIndexerMinimal(ConfReader):

  def __init__(self, global_conf_in, prefix=default_prefix):
    self.last_refresh = datetime.now()
    self.transport_type = 'buffered'  # this is happybase default
    # self.transport_type = 'framed'
    # To store count of batches of updates pushed
    self.dict_up = dict()
    self.pool = None
    self.timeout = 4
    self.batch_update_size = UPDATE_BATCH_SIZE

    # Column families and names
    self.column_list_sha1s = None
    self.extrcf = None
    self.imginfocf = None
    self.imgbuffcf = None
    self.imgbuffcname = None
    self.updateinfocf = None

    super(HBaseIndexerMinimal, self).__init__(global_conf_in, prefix)
    self.set_pp(pp="HBaseIndexerMinimal")
    print('[{}: log] verbose level is: {}'.format(self.pp, self.verbose))

    self.refresh_hbase_conn("__init__")

    # #from thriftpy.transport import TTransportException
    # try:
    #   # The timeout as parameter seems to cause issues?...
    #   self.pool = happybase.ConnectionPool(timeout=60000, size=self.nb_threads,
    #                                        host=self.hbase_host, transport=self.transport_type)
    #   #self.pool = happybase.ConnectionPool(size=self.nb_threads, host=self.hbase_host,
    #   #                                     transport=self.transport_type)
    # #except TTransportException as inst:
    # except TTransportException:
    #   msg = "[{}.read_conf: error] Could not initalize connection to HBase."
    #   print(msg.format(self.pp))
    #   #raise inst

      # # Extractions configuration (TO BE IMPLEMENTED)
      # self.extractions_types = self.get_param('extractions_types')
      # self.extractions_columns = self.get_param('extractions_columns')
      # if len(self.extractions_columns) != len(self.extractions_types):
      #   msg = "[{}.read_conf: error] Dimension mismatch {} vs. {} for columns vs. types"
      #   raise ValueError(msg.format(self.pp, len(self.extractions_columns),
      #                    len(self.extractions_types)))
    # # Could all the other columns be set similarly? And this change be effective everywhere?
    # self.column_list_sha1s = self.get_param("column_list_sha1s", default=column_list_sha1s)
    # self.extrcf = self.get_param("extr_family_column", default=extraction_column_family)
    # self.updateinfocf = self.get_param("update_info_column_family",
    #                                    default=update_info_column_family)
    # self.imginfocf = self.get_param("update_info_column_family", default=img_info_column_family)
    # # columns update. these could be get_col_upproc(), get_col_upstart(), get_col_upcomp()
    # self.column_update_processed = self.updateinfocf + ":" + update_str_processed
    # self.column_update_started = self.updateinfocf + ":" + update_str_started
    # self.column_update_completed = self.updateinfocf + ":" + update_str_completed

  # def check_updateinfocf(self, call_fn=""):
  #   if self.updateinfocf is None:
  #     msg = "[{}.{}: error] update_info_column_family was not set."
  #     raise ValueError(msg.format(self.pp, call_fn))
  #   return True
  #
  # def check_extrcf(self, call_fn=""):
  #   if self.extrcf is None:
  #     msg = "[{}.{}: error] extr_family_column was not set."
  #     raise ValueError(msg.format(self.pp, call_fn))
  #   return True

  # Expose all column names
  def get_dictcf_sha1_table(self):
    return {self.imginfocf: dict(), self.extrcf: dict(), self.imgbuffcf: dict()}

  def get_dictcf_update_table(self):
    return {self.updateinfocf: dict()}

  def get_col_upproc(self):
    #if self.check_updateinfocf():
    return self.updateinfocf + ":" + update_str_processed

  def get_col_upstart(self):
    #if self.check_updateinfocf():
    return self.updateinfocf + ":" + update_str_started

  def get_col_upcomp(self):
    #if self.check_updateinfocf():
    return self.updateinfocf + ":" + update_str_completed

  def get_col_upcreate(self):
    return self.updateinfocf + ":" + UPDATE_STR_CREATED

  def get_col_imgurl(self):
    return self.imginfocf + ":" + "s3_url"

  def get_col_imgurlbak(self):
    return self.imginfocf + ":" + "location"

  def get_col_imgpath(self):
    return self.imginfocf + ":" + "img_path"

  def get_col_imgbuff(self):
    return self.imgbuffcf + ":" + self.imgbuffcname

  def get_cols_listsha1s(self):
    # built as self.updateinfocf + ":" + "list_sha1s"
    #return self.column_list_sha1s
    #return self.updateinfocf + ":" + "list_sha1s"
    return self.updateinfocf + ":" + self.column_list_sha1s


  # # Rename to get_col_extrcheck(self, extraction) for consistency
  # # Who calls that? Nobody. DEPRECATED
  # def get_check_column(self, extraction):
  #   # changed to: self.extrcf
  #   #return extraction_column_family+":"+"_".join([extraction, extr_str_processed])
  #   return self.extrcf + ":" + "_".join([extraction, extr_str_processed])


  # def set_pp(self, pp=None):
  #   self.pp = "HBaseIndexerMinimal"

  def read_conf(self):
    """ Reads configuration parameters.

    Will read parameters 'host', 'table_sha1infos'...
    from self.global_conf.
    """
    super(HBaseIndexerMinimal, self).read_conf()
    # HBase conf
    self.hbase_host = str(self.get_required_param('host'))
    # Get table of images and updates
    self.table_sha1infos_name = str(self.get_required_param('table_sha1infos'))
    self.table_updateinfos_name = str(self.get_param('table_updateinfos'))
    if self.verbose > 0:
      msg = "[{}.read_conf: info] HBase tables name: {} (sha1infos), {} (updateinfos)"
      print(msg.format(self.pp, self.table_sha1infos_name, self.table_updateinfos_name))

    self.nb_threads = int(self.get_param('pool_thread', default=1))
    self.batch_update_size = int(self.get_param('batch_update_size', default=UPDATE_BATCH_SIZE))

    # Can all columns be set similarly? And is this change effective everywhere?
    self.column_list_sha1s = self.get_param("column_list_sha1s", default=column_list_sha1s)
    self.extrcf = self.get_param("extr_family_column", default=EXTR_CF)
    self.imginfocf = self.get_param("image_info_column_family", default=IMG_INFOCF)
    self.imgbuffcf = self.get_param("image_buffer_column_family", default=IMG_BUFFCF)
    self.imgbuffcname = self.get_param("image_buffer_column_name", default=IMG_BUFFCNAME)
    self.updateinfocf = self.get_param("update_info_column_family", default=UPDATE_INFOCF)



  def refresh_hbase_conn(self, calling_function, sleep_time=0):
    # this can take up to 4 seconds...
    try:
      start_refresh = time.time()
      dt_iso = datetime.utcnow().isoformat()
      #msg = "[{}.{}: {}] caught timeout error. Trying to refresh connection pool."
      msg = "[{}.{}: {}] Trying to refresh connection pool."
      print(msg.format(self.pp, calling_function, dt_iso))
      sys.stdout.flush()
      time.sleep(sleep_time)
      # This can hang for a long time?
      # add timeout (in ms: http://happybase.readthedocs.io/en/latest/api.html#connection)?
      #self.pool = happybase.ConnectionPool(size=self.nb_threads, host=self.hbase_host,
      # transport=self.transport_type)
      self.pool = happybase.ConnectionPool(timeout=60000, size=self.nb_threads,
                                           host=self.hbase_host, transport=self.transport_type)
      msg = "[{}.refresh_hbase_conn: log] Refreshed connection pool in {}s."
      print(msg.format(self.pp, time.time() - start_refresh))
      sys.stdout.flush()
    except TTransportException as inst:
      msg = "[{}.read_conf: error] Could not initialize connection to HBase ({})"
      print(msg.format(self.pp, inst))
      sys.stdout.flush()
      #raise inst

  def check_errors(self, previous_err, function_name, inst=None):
    if previous_err >= MAX_ERRORS:
      msg = "[{}: error] function {} reached maximum number of error {}. Error {} was: {}"
      raise Exception(msg.format(self.pp, function_name, MAX_ERRORS, type(inst), inst))
    return None

  # TODO: use column_family from indexer? How to decide which one?
  # Should we not set a default parameter?
  #def get_create_table(self, table_name, conn=None, families={'info': dict()}):
  def get_create_table(self, table_name, conn=None, families=None):
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
        # act differently based on error type (connection issue or actually table missing)
        if type(inst) == TTransportException:
          raise inst
        else:
          if families is None:
            msg = "[{}.get_create_table: ERROR] table {} does not exist and 'families' not provided"
            raise ValueError(msg.format(self.pp, table_name))
          msg = "[{}.get_create_table: info] table {} does not exist (yet): {}{}"
          print(msg.format(self.pp, table_name, type(inst), inst))
          conn.create_table(table_name, families)
          table = conn.table(table_name)
          msg = "[{}.get_create_table: info] created table {}"
          print(msg.format(self.pp, table_name))
          return table
    except Exception as inst:
      # May also fail if families in dictionary do not match those of an existing table,
      # or because of connection issues
      raise inst

  # use http://happybase.readthedocs.io/en/latest/api.html?highlight=scan#happybase.Table.scan?
  def scan_with_prefix(self, table_name, row_prefix=None, columns=None, maxrows=10, previous_err=0,
                       inst=None):
    self.check_errors(previous_err, "scan_with_prefix", inst)
    try:
      with self.pool.connection(timeout=self.timeout) as connection:
        hbase_table = connection.table(table_name)
        # scan table for rows with row_prefix, accumulate in rows information of requested columns
        rows = []
        for one_row in hbase_table.scan(row_prefix=row_prefix, columns=columns, batch_size=10):
          # print "one_row:",one_row
          rows.extend((one_row,))
          if len(rows) >= maxrows:
            return rows
          if self.verbose:
            print("[{}.scan_with_prefix: log] got {} rows.".format(self.pp, len(rows)))
            sys.stdout.flush()
        return rows
    except Exception as inst:
      print("[{}.scan_with_prefix: error] {}".format(self.pp, inst))
      # try to force longer sleep time...
      self.refresh_hbase_conn("scan_with_prefix", sleep_time=4 * previous_err)
      return self.scan_with_prefix(table_name, row_prefix=row_prefix, columns=columns,
                                   maxrows=maxrows, previous_err=previous_err + 1, inst=inst)

  def scan_from_row(self, table_name, row_start=None, columns=None, maxrows=10, previous_err=0,
                    inst=None):
    self.check_errors(previous_err, "scan_from_row", inst)
    try:
      with self.pool.connection(timeout=self.timeout) as connection:
        hbase_table = connection.table(table_name)
        # scan table from row_start, accumulate in rows the information of needed columns
        rows = []
        #for one_row in hbase_table.scan(row_start=row_start, columns=columns, batch_size=2):
        for one_row in hbase_table.scan(row_start=row_start, columns=columns, batch_size=maxrows):
          #print "one_row:",one_row[0]
          rows.extend((one_row,))
          if len(rows) >= maxrows:
            return rows
          if self.verbose > 5:
            print("[{}.scan_from_row: log] got {} rows.".format(self.pp, len(rows)))
            sys.stdout.flush()
        return rows
    except Exception as inst:
      print("[{}.scan_from_row: error] {}".format(self.pp, inst))
      # try to force longer sleep time...
      self.refresh_hbase_conn("scan_from_row", sleep_time=4*previous_err)
      return self.scan_from_row(table_name, row_start=row_start, columns=columns, maxrows=maxrows,
                                previous_err=previous_err + 1, inst=inst)

  # def get_updates_from_date(self, start_date, extr_type="", maxrows=10, previous_err=0,
  # inst=None):
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
  #     return self.get_updates_from_date(start_date, extr_type=extr_type, maxrows=maxrows,
  # previous_err=previous_err+1, inst=inst)
  #   return rows

  def get_updates_from_date(self, start_date, extr_type="", maxrows=MAX_ROWS, previous_err=0,
                            inst=None):
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
                out_rows.append((row[0], row[1]))
          else:
            out_rows = rows
          if out_rows:
            yield out_rows
          # add '~' to exclude last row from next batch
          row_start = rows[-1][0]+'~'
        else:
          #print "[{}.get_updates_from_date: log] 'rows' was None.".format(self.pp)
          break
    except Exception as this_inst: # try to catch any exception
      print("[{}.get_updates_from_date: error] {}".format(self.pp, this_inst))
      self.refresh_hbase_conn("get_updates_from_date")
      try:
        yield self.get_updates_from_date(start_date, extr_type=extr_type, maxrows=maxrows,
                                         previous_err=previous_err+1, inst=this_inst)
      except Exception as new_inst:
        raise new_inst


  def get_unprocessed_updates_from_date(self, start_date, extr_type="", maxrows=MAX_ROWS,
                                        previous_err=0, inst=None):
    # start_date should be in format YYYY-MM-DD(_XX)
    fn = "get_unprocessed_updates_from_date"
    rows = None
    continue_scan = True
    self.check_errors(previous_err, fn, inst)
    nb_rows_scanned = 0
    update_suffix = extr_type + "_" + start_date

    try:
      while continue_scan:
        # build row_start as index_update_YYYY-MM-DD
        row_start = update_prefix + update_suffix

        if self.verbose > 3:
          msg = "[{}.{}: log] row_start is: {}"
          print(msg.format(self.pp, fn, row_start))

        tmp_rows = self.scan_from_row(self.table_updateinfos_name, row_start=row_start,
                                      columns=None, maxrows=maxrows, previous_err=0, inst=None)
        if tmp_rows:
          nb_rows_scanned += len(tmp_rows)
          if self.verbose > 2:
            msg = "[{}.{}: log] Scanned {} rows so far."
            print(msg.format(self.pp, fn, nb_rows_scanned))
          for row_id, row_val in tmp_rows:
            last_row = row_id
            # This fails for update from spark...
            #start_date = '_'.join(last_row.split('_')[-2:])
            # Does this work for any type of update?
            update_suffix = '_'.join(last_row.split('_')[2:])+'~'
            # changed to: self.column_update_processed
            #if info_column_family + ":" + update_str_processed not in row_val:
            #if self.column_update_processed not in row_val:
            if self.get_col_upproc() not in row_val:
              if extr_type and extr_type not in row_id:
                continue
              if rows is None:
                rows = [(row_id, row_val)]
              else:
                rows.append((row_id, row_val))
        else:
          continue_scan = False

        if rows:
          if tmp_rows is None or len(tmp_rows) < maxrows:
            # Looks like we reach the end of updates list
            continue_scan = False
          yield rows

    except Exception as inst: # try to catch any exception
      full_trace_error("[{}.{}: error] {}".format(self.pp, fn, inst))
      self.refresh_hbase_conn(fn, sleep_time=4*previous_err)
      yield self.get_unprocessed_updates_from_date(start_date, extr_type=extr_type, maxrows=maxrows,
                                                   previous_err=previous_err+1, inst=inst)


  def get_missing_extr_updates_from_date(self, start_date, extr_type="", maxrows=MAX_ROWS,
                                         previous_err=0, inst=None):
    fn = "get_missing_extr_updates_from_date"
    # This induces that we keep reprocessing images that cannot be downloaded/processed every time
    # we check for updates...
    if not extr_type:
      msg = "[{}.{}: warning] extr_type was not specified."
      print(msg.format(self.pp, fn))
      return

    # start_date should be in format YYYY-MM-DD(_XX)
    self.check_errors(previous_err, fn, inst)
    # build row_start as index_update_YYYY-MM-DD
    update_suffix = extr_type + "_" + start_date
    next_start_date = start_date

    try:
      while True:
        # build row_start as index_update_YYYY-MM-DD
        row_start = update_prefix + update_suffix
        if self.verbose > 3:
          msg = "[{}.{}: log] row_start is: {}"
          print(msg.format(self.pp, fn, row_start))

        rows = self.scan_from_row(self.table_updateinfos_name, row_start=row_start, maxrows=maxrows)
        if rows:
          # Filter out updates of other extractions type
          for row in rows:
            out_rows = []
            update_suffix = '_'.join(row[0].split('_')[2:]) + '~'
            if extr_type in row[0]:
              # changed to: self.column_update_completed
              #if update_completed_column in row[1]:
              #if self.column_update_completed in row[1]:
              if self.get_col_upcomp() in row[1]:
                # Update has been marked as all extractions being performed
                continue
              # TODO: should we store in a set all checked updated for missing extractions
              # so we only process them once in the life of the indexer?
              if self.verbose > 4:
                msg = "[{}.{}: log] checking update {} for missing extractions"
                print(msg.format(self.pp, fn, row[0]))
              if column_list_sha1s in row[1]:
                tmp_list_sha1s = row[1][column_list_sha1s].split(',')
                missing_extr_sha1s = self.get_missing_extr_sha1s(tmp_list_sha1s, extr_type)
                if missing_extr_sha1s:
                  if self.verbose > 5:
                    msg = "[{}.{}: log] update {} has missing extractions"
                    print(msg.format(self.pp, fn, row[0]))
                  out_row_val = dict()
                  out_row_val[column_list_sha1s] = ','.join(missing_extr_sha1s)
                  if out_rows:
                    out_rows.append((row[0], out_row_val))
                  else:
                    out_rows = [(row[0], out_row_val)]
                else:
                  if self.verbose > 4:
                    msg = "[{}.{}: log] update {} has no missing extractions"
                    print(msg.format(self.pp, fn, row[0]))
                  # We should mark as completed here
                  # changed to: self.column_update_completed
                  #update_completed_dict = {row[0]:
                  # {info_column_family + ':' + update_str_completed: str(1)}}
                  #update_completed_dict = {row[0]: {self.column_update_completed: str(1)}}
                  update_completed_dict = {row[0]: {self.get_col_upcomp(): str(1)}}
                  self.push_dict_rows(dict_rows=update_completed_dict,
                                      table_name=self.table_updateinfos_name)
              else:
                msg = "[{}.{}: warning] update {} has no images list"
                print(msg.format(self.pp, fn, row[0]))
            if out_rows:
              yield out_rows

        else:
          # We have reached the end of the scan
          break
    except Exception as inst: # try to catch any exception
      print("[{}.{}: error] {}".format(self.pp, fn, inst))
      self.refresh_hbase_conn(fn)
      yield self.get_missing_extr_updates_from_date(next_start_date, extr_type=extr_type,
                                                    maxrows=maxrows, previous_err=previous_err+1,
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
    """ Push a dictionary to the HBase 'table_name' assuming keys are the row keys and
    each entry is a valid dictionary containing the column names and values.

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
        batch = hbase_table.batch(batch_size=batch_size) # should we have a bigger batch size?
        # Assume dict_rows[k] is a dictionary ready to be pushed to HBase...
        for row_key in dict_rows:
          if previous_err > 1:
            tmp_dict_row = dict_rows[row_key]
            row_size = sys.getsizeof(tmp_dict_row)
            for key in tmp_dict_row:
              row_size += sys.getsizeof(tmp_dict_row[key])
            if row_size > MAX_ROW_SIZE: # print warning if size is bigger than 2MB?
              msg = "[{}: warning] Row {} size seems to be: {}. Keys are: {}"
              print(msg.format(self.pp, row_key, row_size, tmp_dict_row.keys()))
              sys.stdout.flush()
              # Try to discard buffer to avoid 'KeyValue size too large'
              if img_buffer_column in tmp_dict_row:
                del tmp_dict_row[img_buffer_column]
            batch.put(row_key, tmp_dict_row)
          else:
            batch.put(row_key, dict_rows[row_key])
        batch.send()
        return True

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
      if previous_err+1 == MAX_ERRORS:
        msg = "[push_dict_rows: log] dict_rows keys: {}"
        print(msg.format(dict_rows.keys()))
      self.refresh_hbase_conn("push_dict_rows", sleep_time=4*previous_err)
      return self.push_dict_rows(dict_rows, table_name, families=families,
                                 previous_err=previous_err+1, inst=inst)

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
          for batch_start in range(0, len(list_queries), READ_BATCH_SIZE):
            batch_end = min(batch_start+READ_BATCH_SIZE, len(list_queries))
            batch_list_queries = list_queries[batch_start:batch_end]
            rows.extend(hbase_table.rows(batch_list_queries, columns=columns))
            nb_batch += 1
          if self.verbose > 5:
            msg = "[get_rows_by_batch: log] got {} rows using {} batches."
            print(msg.format(len(rows), nb_batch))
          return rows
        else:
          msg = "[get_rows_by_batch: error] could not get table: {} (families: {})"
          raise ValueError(msg.format(table_name, families))
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
        rows = self.get_rows_by_batch(list_sha1s, self.table_sha1infos_name, families=families,
                                      columns=columns)
      except Exception as inst: # try to catch any exception
        print("[get_columns_from_sha1_rows: error] {}".format(inst))
        self.refresh_hbase_conn("get_columns_from_sha1_rows")
        return self.get_columns_from_sha1_rows(list_sha1s, columns, families=families,
                                               previous_err=previous_err+1, inst=inst)
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
    # changed to: self.extrcf
    #rows = self.get_columns_from_sha1_rows(list_sha1s, columns=[extraction_column_family])
    rows = self.get_columns_from_sha1_rows(list_sha1s, columns=[self.extrcf])
    samples_id = []
    feats = []
    for row in rows:
      for k in row[1]:
        #print k
        # changed to: self.extrcf
        #if k.startswith(extraction_column_family+":"+extr_type) and not k.endswith("_updateid") and not k.endswith(extr_str_processed):
        if k.startswith(self.extrcf + ":" + extr_type) and not k.endswith("_updateid") and not k.endswith(EXTR_STR_PROCESSED):
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
      print("[{}: info] Got {} rows and {} features.".format(self.pp, len(rows), len(samples_id)))
    return samples_id, feats

  def get_missing_extr_sha1s(self, list_sha1s, extr_type):
    # changed to: self.extrcf
    #rows = self.get_columns_from_sha1_rows(list_sha1s, columns=[extraction_column_family])
    rows = self.get_columns_from_sha1_rows(list_sha1s, columns=[self.extrcf])
    sha1s_w_extr = set()
    for row in rows:
      for key in row[1]:
        # changed to: self.extrcf
        #if key.startswith(extraction_column_family+":"+extr_type) and key.endswith(extr_str_processed):
        if key.startswith(self.extrcf + ":" + extr_type) and key.endswith(EXTR_STR_PROCESSED):
          sha1s_w_extr.add(str(row[0]))
    #print "Found {} sha1s with extractions".format(len(sha1s_w_extr))
    return list(set(list_sha1s) - sha1s_w_extr)
