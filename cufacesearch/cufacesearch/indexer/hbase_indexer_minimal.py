from __future__ import print_function

import sys
import time
from datetime import datetime

import happybase
from thriftpy.transport import TTransportException

from ..common.conf_reader import ConfReader
from ..common.error import full_trace_error
from ..common import update_prefix


# Variables exposed in configuration, default settings are those used before transition:
UPDATE_INFOCF = "info"
UPDATE_LISTSHA1CNAME = "list_sha1s"
IMG_INFOCF = "info"
IMG_BUFFCF = "info"
IMG_BUFFCNAME = "img_buffer"
IMG_URLCNAME = "s3_url"
EXTR_CF = "ext"
DEFAULT_HBASEINDEXER_PREFIX = "HBI_"
SKIP_FAILED = False
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

# Not yet exposed in configuration
EXTR_STR_PROCESSED = "processed"
EXTR_STR_FAILED= "failed"
UPDATE_STR_PROCESSED = "processed"
UPDATE_STR_STARTED = "started"
UPDATE_STR_CREATED = "created"
UPDATE_STR_COMPLETED = "completed"
IMG_URLBACKUPCNAME = "location"
IMG_PATHCNAME = "img_path"


# After transition
# Put everything in "data" column family and img:img for buffer but we SHOULD NOT write to it
# data vs. info, double check the consequences downstream.
# should we have an "info_column_family" and an "image_info_column_family"
# Uncharted format data:location for s3_url but we should not overwrite...
# we could use it as backup for getting s3_url?
# Could all these be specified dynamically, e.g. in conf file?
# img_info_column_family = "data"
# img_buffer_column_family = "img"
# extraction_column_family = "data"
# update_info_column_family = "info" # does not really matter
# update_completed_column = update_info_column_family+":"+update_str_completed
# img_buffer_column = img_buffer_column_family+":img"
# img_URL_column = img_info_column_family+":s3_url"
# img_backup_URL_column = img_info_column_family+":location"
# img_path_column = img_info_column_family+":img_path"
# default_prefix = "HBI_"
# MAX_ROWS = 500


class HBaseIndexerMinimal(ConfReader):
  """Indexing image and update information using HBase as backend.
  """

  def __init__(self, global_conf_in, prefix=DEFAULT_HBASEINDEXER_PREFIX):
    self.last_refresh = datetime.now()
    self.transport_type = 'buffered' # this is happybase default

    # To store count of batches of updates pushed
    self.dict_up = dict()
    self.pool = None
    self.timeout = 4
    self.batch_update_size = UPDATE_BATCH_SIZE

    # Column families and names
    self.extrcf = None
    self.imginfocf = None
    self.imgbuffcf = None
    self.imgbuffcname = None
    self.updateinfocf = None
    self.updatelistsha1scname = None
    self.skipfailed = None

    super(HBaseIndexerMinimal, self).__init__(global_conf_in, prefix)
    self.set_pp(pp="HBaseIndexerMinimal")
    print('[{}: log] verbose level is: {}'.format(self.pp, self.verbose))

    self.refresh_hbase_conn("__init__")


  # Expose all column names so they can be defined in configuration.
  def get_dictcf_sha1_table(self):
    """Get dictionary of column families for images table.

    :return: dictionary of column families
    :rtype: dict
    """
    return {self.imginfocf: dict(), self.extrcf: dict(), self.imgbuffcf: dict()}

  def get_dictcf_update_table(self):
    """Get dictionary of column families for updates table.

    :return: dictionary of column families
    :rtype: dict
    """
    return {self.updateinfocf: dict()}

  def get_col_upproc(self):
    """Get full column (i.e. ``column_family:column_name``) for storing update processing end date.

    :return: column update processed
    :rtype: string
    """
    return self.updateinfocf + ":" + UPDATE_STR_PROCESSED

  def get_col_upstart(self):
    """Get full column (i.e. ``column_family:column_name``) for storing update processing start date.

    :return: column update started
    :rtype: string
    """
    return self.updateinfocf + ":" + UPDATE_STR_STARTED

  def get_col_upcomp(self):
    """Get full column (i.e. ``column_family:column_name``) for storing update completion status.

    :return: column update completed
    :rtype: string
    """
    return self.updateinfocf + ":" + UPDATE_STR_COMPLETED

  def get_col_upcreate(self):
    """Get full column (i.e. ``column_family:column_name``) for storing update creation date.

    :return: column update completed
    :rtype: string
    """
    return self.updateinfocf + ":" + UPDATE_STR_CREATED

  def get_col_imgurl(self):
    """Get full column (i.e. ``column_family:column_name``) for storing image URL.

    :return: column image URL
    :rtype: string
    """
    return self.imginfocf + ":" + self.imgurlcname

  def get_col_imgurlbak(self):
    """Get full column (i.e. ``column_family:column_name``) for storing image URL (backup).

    :return: column image URL (backup)
    :rtype: string
    """
    return self.imginfocf + ":" + IMG_URLBACKUPCNAME

  def get_col_imgpath(self):
    """Get full column (i.e. ``column_family:column_name``) for storing image path.

    :return: column image path
    :rtype: string
    """
    return self.imginfocf + ":" + IMG_PATHCNAME

  def get_col_imgbuff(self):
    """Get full column (i.e. ``column_family:column_name``) for storing image buffer.

    :return: column image buffer
    :rtype: string
    """
    return self.imgbuffcf + ":" + self.imgbuffcname

  def get_col_listsha1s(self):
    """Get full column (i.e. ``column_family:column_name``) for storing update images sha1 list.

    :return: column sha1 list
    :rtype: string
    """
    return self.updateinfocf + ":" + self.updatelistsha1scname

  def read_conf(self):
    """Reads configuration parameters from self.global_conf.

    Required parameters are:

    - ``host``
    - ``table_sha1infos``

    Optional parameters are:

    - ``table_updateinfos``
    - ``pool_thread``
    - ``batch_update_size``
    - ``column_list_sha1s``
    - ``extr_family_column``
    - ``image_info_column_family``
    - ``image_buffer_column_family``
    - ``image_buffer_column_name``
    - ``update_info_column_family``
    """
    super(HBaseIndexerMinimal, self).read_conf()
    # HBase conf
    self.hbase_host = str(self.get_required_param('host'))
    # Get table of images and updates
    self.table_sha1infos_name = str(self.get_required_param('table_sha1infos'))
    # Optional for "IN" indexer for example
    self.table_updateinfos_name = str(self.get_param('table_updateinfos'))
    if self.verbose > 0:
      msg = "[{}.read_conf: info] HBase tables name: {} (sha1infos), {} (updateinfos)"
      print(msg.format(self.pp, self.table_sha1infos_name, self.table_updateinfos_name))

    self.nb_threads = int(self.get_param('pool_thread', default=1))
    self.batch_update_size = int(self.get_param('batch_update_size', default=UPDATE_BATCH_SIZE))
    self.skipfailed = self.get_param("skip_failed", default=SKIP_FAILED)

    # Can all columns be set similarly? And is this change effective everywhere?
    self.updatelistsha1scname = self.get_param("column_list_sha1s", default=UPDATE_LISTSHA1CNAME)
    self.extrcf = self.get_param("extr_family_column", default=EXTR_CF)
    self.imginfocf = self.get_param("image_info_column_family", default=IMG_INFOCF)
    self.imgbuffcf = self.get_param("image_buffer_column_family", default=IMG_BUFFCF)
    self.imgurlcname = self.get_param("image_url_column_name", default=IMG_URLCNAME)
    self.imgbuffcname = self.get_param("image_buffer_column_name", default=IMG_BUFFCNAME)
    self.updateinfocf = self.get_param("update_info_column_family", default=UPDATE_INFOCF)

  def refresh_hbase_conn(self, calling_function, sleep_time=0):
    """Refresh connection to HBase.

    :param calling_function: name of function calling
    :param sleep_time: time to sleep before refreshing.
    """
    # NB: This can take up to 4 seconds sometimes...
    try:
      start_refresh = time.time()
      dt_iso = datetime.utcnow().isoformat()
      msg = "[{}.{}: {}] Trying to refresh connection pool."
      print(msg.format(self.pp, calling_function, dt_iso))
      sys.stdout.flush()
      time.sleep(sleep_time)
      self.pool = happybase.ConnectionPool(timeout=60000, size=self.nb_threads,
                                           host=self.hbase_host, transport=self.transport_type)
      msg = "[{}.refresh_hbase_conn: log] Refreshed connection pool in {}s."
      print(msg.format(self.pp, time.time() - start_refresh))
      sys.stdout.flush()
    except TTransportException as inst:
      msg = "[{}.read_conf: error] Could not initialize connection to HBase ({})"
      print(msg.format(self.pp, inst))
      sys.stdout.flush()

  def check_errors(self, nb_err, function_name, inst=None):
    """Check if function "function_name" has reached MAX_ERRORS errors.
    Raise error if that is the case.

    :param nb_err: number of errors caught in function "function_name"
    :param function_name: name of the function for which we want to check the error count.
    :param inst: error instance.
    :raises Exception: if nb_err >= MAX_ERRORS
    """
    if nb_err >= MAX_ERRORS:
      msg = "[{}: error] function {} reached maximum number of error {}. Error {} was: {}"
      raise Exception(msg.format(self.pp, function_name, MAX_ERRORS, type(inst), inst))
    #return None

  def get_create_table(self, table_name, conn=None, families=None):
    """Get HBase table "table_name", creating it if it does not exist yet.

    :param table_name: name of the table to create.
    :type table_name: string
    :param conn: happybase connection
    :type conn: :class:`happybase.Connection`
    :param families: dictionary of column families (see ``get_dictcf_sha1_table`` and ``get_dictcf_update_table``)
    :type families: dict
    :return: table
    :rtype: :class:`happybase.Table`
    """
    # try:
    if conn is None:
      from happybase.connection import Connection
      conn = Connection(self.hbase_host)
    try:
      # as no exception would be raised if table does not exist...
      table = conn.table(table_name)
      # ...try to access families to get error if table does not exist yet
      _ = table.families()
      # table exist, return it
      return table
    except Exception as inst:
      # act differently based on error type (connection issue or actually table missing)
      if type(inst) == TTransportException:
        raise inst
      else:
        # we need to create the table
        msg = "[{}.get_create_table: info] table {} does not exist (yet): {}{}"
        print(msg.format(self.pp, table_name, type(inst), inst))
        # but we need to know which column families it should contain
        if families is None:
          msg = "[{}.get_create_table: ERROR] table {} does not exist and 'families' not provided"
          raise ValueError(msg.format(self.pp, table_name))
        # Create table...
        conn.create_table(table_name, families)
        table = conn.table(table_name)
        msg = "[{}.get_create_table: info] created table {}"
        print(msg.format(self.pp, table_name))
        # ... and return it
        return table
    # except Exception as inst:
    #   # May fail if families in dictionary do not match those of an existing table,
    #   # or because of connection issues. We want to raise up these error.
    #   raise inst

  def scan_from_row(self, table_name, row_start=None, columns=None, maxrows=10, perr=0, inst=None):
    """Scan table "table_name" starting a row "row_start" and retrieving columns "columns".

    :param table_name: name of table to scan
    :type table_name: string
    :param row_start: starting row
    :type row_start: string
    :param columns: columns to retrieve
    :type columns: list
    :param maxrows: maximum number of rows to return
    :type maxrows: int
    :param perr: number of errors caught so far
    :type perr: int
    :param inst: error instance caught
    :type inst: Exception
    :return: rows list
    :rtype: list
    """
    self.check_errors(perr, "scan_from_row", inst)
    try:
      with self.pool.connection(timeout=self.timeout) as connection:
        hbase_table = connection.table(table_name)
        # scan table from row_start, accumulate in rows the information of needed columns
        rows = []
        for one_row in hbase_table.scan(row_start=row_start, columns=columns, batch_size=maxrows):
          rows.extend((one_row,))
          if len(rows) >= maxrows:
            return rows
          if self.verbose > 5:
            print("[{}.scan_from_row: log] got {} rows.".format(self.pp, len(rows)))
            sys.stdout.flush()
        return rows
    except Exception as err_inst:
      #print("[{}.scan_from_row: error] {}".format(self.pp, err_inst))
      # try to force longer sleep time...
      self.refresh_hbase_conn("scan_from_row", sleep_time=10 * perr)
      return self.scan_from_row(table_name, row_start=row_start, columns=columns, maxrows=maxrows,
                                perr=perr + 1, inst=err_inst)

  def get_updates_from_date(self, start_date, extr_type="", maxrows=MAX_ROWS, perr=0, inst=None):
    """Get updates of ``extr_type`` from ``self.table_updateinfos_name`` starting from first update
    after row key build using ``extr_type`` and ``start_date`` as:

    - ``update_prefix + extr_type + "_" + start_date``

    :param start_date: date (formatted as YYYY-MM-DD) from which updates should be retrieved
    :type start_date: string
    :param extr_type: extraction type
    :type extr_type: string
    :param maxrows: maximum number of rows to return
    :type maxrows: int
    :param perr: number of errors caught so far
    :type perr: int
    :param inst: error instance caught
    :type inst: Exception
    :yeild: list of rows of updates.
    """
    # start_date should be in format YYYY-MM-DD(_XX)
    rows = None
    self.check_errors(perr, "get_updates_from_date", inst)
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
          # NB: maxrows is not fully enforced here
          if out_rows:
            yield out_rows
          # add '~' to exclude last row from next batch
          row_start = rows[-1][0]+'~'
        else:
          #print "[{}.get_updates_from_date: log] 'rows' was None.".format(self.pp)
          break
    except Exception as err_inst: # try to catch any exception
      print("[{}.get_updates_from_date: error] {}".format(self.pp, err_inst))
      self.refresh_hbase_conn("get_updates_from_date")
      yield self.get_updates_from_date(start_date, extr_type=extr_type, maxrows=maxrows,
                                       perr=perr + 1, inst=err_inst)


  def get_unprocessed_updates_from_date(self, start_date, extr_type="", maxrows=MAX_ROWS, perr=0,
                                        inst=None):
    """Get unprocessed updates of type "extr_type" from "start_date".

    :param start_date: start date
    :param extr_type: extraction type
    :param maxrows: maximum number of rows
    :param perr: previous errors count
    :param inst: last error instance
    :return:
    """
    # start_date should be in format YYYY-MM-DD(_XX)
    fname = "get_unprocessed_updates_from_date"
    rows = None
    continue_scan = True
    self.check_errors(perr, fname, inst)
    nb_rows_scanned = 0
    update_suffix = extr_type + "_" + start_date

    try:
      while continue_scan:
        # build row_start as index_update_YYYY-MM-DD
        row_start = update_prefix + update_suffix

        if self.verbose > 3:
          msg = "[{}.{}: log] row_start is: {}"
          print(msg.format(self.pp, fname, row_start))

        tmp_rows = self.scan_from_row(self.table_updateinfos_name, row_start=row_start,
                                      columns=None, maxrows=maxrows, perr=0, inst=None)
        if tmp_rows:
          nb_rows_scanned += len(tmp_rows)
          if self.verbose > 2:
            msg = "[{}.{}: log] Scanned {} rows so far."
            print(msg.format(self.pp, fname, nb_rows_scanned))
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

    except Exception as err_inst: # try to catch any exception
      full_trace_error("[{}.{}: error] {}".format(self.pp, fname, err_inst))
      self.refresh_hbase_conn(fname, sleep_time=4 * perr)
      yield self.get_unprocessed_updates_from_date(start_date, extr_type=extr_type, maxrows=maxrows,
                                                   perr=perr + 1, inst=err_inst)


  def get_missing_extr_updates_from_date(self, start_date, extr_type="", maxrows=MAX_ROWS,
                                         perr=0, inst=None):
    """Get updates with missing extraction from "start_date".

    :param start_date: start date
    :param extr_type: extraction type
    :param maxrows: maximum number of rows
    :param perr: previous errors count
    :param inst: last error instance
    :return:
    """
    fname = "get_missing_extr_updates_from_date"
    # This induces that we keep reprocessing images that cannot be downloaded/processed every time
    # we check for updates...
    if not extr_type:
      msg = "[{}.{}: warning] extr_type was not specified."
      print(msg.format(self.pp, fname))
      return

    # start_date should be in format YYYY-MM-DD(_XX)
    self.check_errors(perr, fname, inst)
    # build row_start as index_update_YYYY-MM-DD
    update_suffix = extr_type + "_" + start_date
    next_start_date = start_date

    try:
      while True:
        # build row_start as index_update_YYYY-MM-DD
        row_start = update_prefix + update_suffix
        if self.verbose > 3:
          msg = "[{}.{}: log] row_start is: {}"
          print(msg.format(self.pp, fname, row_start))

        rows = self.scan_from_row(self.table_updateinfos_name, row_start=row_start, maxrows=maxrows)
        if rows:
          # Filter out updates of other extractions type
          for row in rows:
            out_rows = []
            update_suffix = '_'.join(row[0].split('_')[2:]) + '~'
            if extr_type in row[0]:
              if self.get_col_upcomp() in row[1]:
                # Update has been marked as all extractions being performed
                continue
              # TODO: should we store in a set all checked updated for missing extractions
              # so we only process them once in the life of the indexer?
              if self.verbose > 4:
                msg = "[{}.{}: log] checking update {} for missing extractions"
                print(msg.format(self.pp, fname, row[0]))
              if self.get_col_listsha1s() in row[1]:
                tmp_list_sha1s = row[1][self.get_col_listsha1s()].split(',')
                missing_extr_sha1s = self.get_missing_extr_sha1s(tmp_list_sha1s, extr_type,
                                                                 skip_failed=self.skipfailed)
                if missing_extr_sha1s:
                  if self.verbose > 5:
                    msg = "[{}.{}: log] update {} has missing extractions"
                    print(msg.format(self.pp, fname, row[0]))
                  out_row_val = dict()
                  out_row_val[self.get_col_listsha1s()] = ','.join(missing_extr_sha1s)
                  if out_rows:
                    out_rows.append((row[0], out_row_val))
                  else:
                    out_rows = [(row[0], out_row_val)]
                else:
                  if self.verbose > 4:
                    msg = "[{}.{}: log] update {} has no missing extractions"
                    print(msg.format(self.pp, fname, row[0]))
                  # We should mark as completed here
                  if not self.skipfailed:
                    update_completed_dict = {row[0]: {self.get_col_upcomp(): str(1)}}
                    self.push_dict_rows(dict_rows=update_completed_dict,
                                        table_name=self.table_updateinfos_name)
              else:
                msg = "[{}.{}: warning] update {} has no images list"
                print(msg.format(self.pp, fname, row[0]))
            if out_rows:
              yield out_rows

        else:
          # We have reached the end of the scan
          break
    except Exception as err_inst: # try to catch any exception
      print("[{}.{}: error] {}".format(self.pp, fname, err_inst))
      self.refresh_hbase_conn(fname)
      yield self.get_missing_extr_updates_from_date(next_start_date, extr_type=extr_type,
                                                    maxrows=maxrows, perr=perr + 1, inst=err_inst)

  # TODO: move date tools to common
  def get_today_string(self):
    """Get today date formatted as '%Y-%m-%d'

    :return: today string
    """
    return datetime.today().strftime('%Y-%m-%d')

  def get_next_update_id(self, today=None, extr_type=""):
    """Get next valid update id for "extr_type".

    :param today: today string
    :param extr_type: extraction type
    :return: update_id, today string
    """
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



  def push_dict_rows(self, dict_rows, table_name, families=None, perr=0, inst=None):
    """Push a dictionary to the HBase 'table_name' assuming keys are the row keys and
    each entry is a valid dictionary containing the column names and values.

    :param dict_rows: input dictionary to be pushed.
    :param table_name: name of the HBase table where to push the data.
    :param families: all families of the table (if we need to create the table)
    :param perr: previous errors count
    :param inst: last error instance
    :return: None
    """
    self.check_errors(perr, "push_dict_rows", inst)
    batch_size = 10
    if perr > 0:
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

        batch = hbase_table.batch(batch_size=batch_size)
        # Assume dict_rows[k] is a dictionary ready to be pushed to HBase...
        for row_key in dict_rows:
          if perr > 1:
            tmp_dict_row = dict_rows[row_key]
            row_size = sys.getsizeof(tmp_dict_row)
            for key in tmp_dict_row:
              row_size += sys.getsizeof(tmp_dict_row[key])
            if row_size > MAX_ROW_SIZE: # print warning if size is bigger than 2MB?
              msg = "[{}: warning] Row {} size seems to be: {}. Keys are: {}"
              print(msg.format(self.pp, row_key, row_size, tmp_dict_row.keys()))
              sys.stdout.flush()
              # Try to discard buffer to avoid 'KeyValue size too large' error
              if self.get_col_imgbuff() in tmp_dict_row:
                del tmp_dict_row[self.get_col_imgbuff()]
            batch.put(row_key, tmp_dict_row)
          else:
            batch.put(row_key, dict_rows[row_key])
        batch.send()
        return True
    except Exception as err_inst: # try to catch any exception
      # For debugging
      if perr + 1 == MAX_ERRORS:
        msg = "[push_dict_rows: log] dict_rows keys: {}"
        print(msg.format(dict_rows.keys()))
      self.refresh_hbase_conn("push_dict_rows", sleep_time=4 * perr)
      return self.push_dict_rows(dict_rows, table_name, families=families, perr=perr+1,
                                 inst=err_inst)

  def get_rows_by_batch(self, list_queries, table_name, families=None, columns=None, perr=0,
                        inst=None):
    """Get rows with keys "list_queries" from "table_name".

    :param list_queries: list of row keys
    :param table_name: table name
    :param families: column families (in case we need to create the table)
    :param columns: columns to retrieve
    :param perr: previous errors count
    :param inst: last error instance
    :return:
    """
    self.check_errors(perr, "get_rows_by_batch", inst)
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
    except Exception as err_inst:
      if type(err_inst) == ValueError:
        raise err_inst
      # try to force longer sleep time if error repeats...
      self.refresh_hbase_conn("get_rows_by_batch", sleep_time=perr)
      return self.get_rows_by_batch(list_queries, table_name, families=families, columns=columns,
                                    perr=perr+1, inst=err_inst)

  def get_columns_from_sha1_rows(self, list_sha1s, columns, families=None, perr=0, inst=None):
    """Get columns "columns" for images in "list_sha1s".

    :param list_sha1s: list of images sha1
    :param columns: columns to retrieve
    :param families: column families (in case we need to create the table)
    :param perr: previous errors count
    :param inst: last error instance
    :return: rows of images filled with requested columns
    """
    rows = None
    self.check_errors(perr, "get_columns_from_sha1_rows", inst)
    if list_sha1s:
      try:
        rows = self.get_rows_by_batch(list_sha1s, self.table_sha1infos_name, families=families,
                                      columns=columns)
      except Exception as err_inst: # try to catch any exception
        print("[get_columns_from_sha1_rows: error] {}".format(err_inst))
        self.refresh_hbase_conn("get_columns_from_sha1_rows")
        return self.get_columns_from_sha1_rows(list_sha1s, columns, families=families,
                                               perr=perr + 1, inst=err_inst)
    return rows


  def get_features_from_sha1s(self, list_sha1s, extr_type, feat_type_decode=None):
    """ Get features of "extr_type" for images in "list_sha1s"

    :param list_sha1s: list of images sha1
    :param extr_type: extraction type.
    :param feat_type_decode: type of feature (to know how to decode in featB64decode)
    :return: (samples_id, feats) tuple of list of sample ids and corresponding features
    """
    from ..featurizer.featsio import featB64decode

    # Cannot use column filters here...
    has_detection = False
    if "_".join(extr_type.split("_")[-2:]) != "full_image":
      has_detection = True
    # sbpycaffe is saved as np.float32 while dlib face features are np.float64
    if feat_type_decode is None:
      feat_type_decode = extr_type.split("_")[0]
    # We need to get all extractions and parse them for matches with extr_type...
    # We could read image infos if we need to filter things out based on format and/or image size

    rows = self.get_columns_from_sha1_rows(list_sha1s, columns=[self.extrcf])
    samples_id = []
    feats = []
    for row in rows:
      for key in row[1]:
        notinfocol = not key.endswith("_updateid") and \
                     not key.endswith(EXTR_STR_PROCESSED) and not key.endswith(EXTR_STR_FAILED)
        if key.startswith(self.extrcf + ":" + extr_type) and notinfocol:
          # Get sample id
          if not has_detection:
            sid = str(row[0])
          else:
            # parse to get id, sha1 + detection_box
            sid = str(row[0])+"_"+"_".join(key.split("_")[4:8])
          # Get feature
          try:
            feat = featB64decode(row[1][key], feat_type_decode)
            # Add sample id and feature
            samples_id.append(sid)
            feats.append(feat)
          except Exception as inst:
            msg = "[{}.get_features_from_sha1s: log] Failed to get feature for image {}"
            print(msg.format(self.pp, row[0]))
            #full_trace_error(msg.format(self.pp, row[0]))
            #raise inst
    if self.verbose > 0:
      print("[{}: info] Got {} rows and {} features.".format(self.pp, len(rows), len(samples_id)))
    return samples_id, feats

  def get_missing_extr_sha1s(self, list_sha1s, extr_type, skip_failed=False):
    """Get list of images sha1 for which "extr_type" was not computed.

    :param list_sha1s: list of images sha1
    :param extr_type: extraction type.
    :return: list of sha1 without extraction
    """
    rows = self.get_columns_from_sha1_rows(list_sha1s, columns=[self.extrcf])
    sha1s_w_extr = set()
    for row in rows:
      for key in row[1]:
        kstart = key.startswith(self.extrcf + ":" + extr_type)
        kfailed = (skip_failed and key.endswith(EXTR_STR_FAILED) and row[1][key]==str(1))
        kend = key.endswith(EXTR_STR_PROCESSED) or kfailed
        if kstart and kend:
          sha1s_w_extr.add(str(row[0]))
    return list(set(list_sha1s) - sha1s_w_extr)


  # DEPRECATED
  # # use http://happybase.readthedocs.io/en/latest/api.html?highlight=scan#happybase.Table.scan?
  # def scan_with_prefix(self, table_name, row_prefix=None, columns=None, maxrows=10, perr=0,
  #                      inst=None):
  #   self.check_errors(perr, "scan_with_prefix", inst)
  #   try:
  #     with self.pool.connection(timeout=self.timeout) as connection:
  #       hbase_table = connection.table(table_name)
  #       # scan table for rows with row_prefix, accumulate in rows information of requested columns
  #       rows = []
  #       for one_row in hbase_table.scan(row_prefix=row_prefix, columns=columns, batch_size=10):
  #         # print "one_row:",one_row
  #         rows.extend((one_row,))
  #         if len(rows) >= maxrows:
  #           return rows
  #         if self.verbose:
  #           print("[{}.scan_with_prefix: log] got {} rows.".format(self.pp, len(rows)))
  #           sys.stdout.flush()
  #       return rows
  #   except Exception as inst:
  #     print("[{}.scan_with_prefix: error] {}".format(self.pp, inst))
  #     # try to force longer sleep time...
  #     self.refresh_hbase_conn("scan_with_prefix", sleep_time=4 * perr)
  #     return self.scan_with_prefix(table_name, row_prefix=row_prefix, columns=columns,
  #                                  maxrows=maxrows, previous_err=perr + 1, inst=inst)

  # # DEPRECATED
  # def get_batch_update(self, list_sha1s):
  #   l = len(list_sha1s)
  #   for ndx in range(0, l, self.batch_update_size):
  #     yield list_sha1s[ndx:min(ndx + self.batch_update_size, l)]

  # # DEPRECATED
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

  # # DEPRECATED
  # def push_list_updates(self, list_sha1s, update_id):
  #   """ Push the 'update_id' composed of the images in 'list_sha1s' to 'table_updateinfos_name'.
  #
  #   :param list_sha1s: list of the images SHA1
  #   :param update_id: update identifier
  #   """
  #   # Build update dictionary
  #   dict_updates = dict()
  #   dict_updates[update_id] = {self.get_col_listsha1s(): ','.join(list_sha1s)}
  #   # Push it
  #   self.push_dict_rows(dict_updates, self.table_updateinfos_name)
