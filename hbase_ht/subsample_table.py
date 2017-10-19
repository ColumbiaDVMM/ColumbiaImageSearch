import happybase
import json
import time
import os
import sys
from argparse import ArgumentParser

TTransportException = happybase._thriftpy.transport.TTransportException
#sys.path.insert(0, os.path.abspath('../cu_image_search/memex_tools'))

hbase_conn_timeout = None
nb_threads = 1
pool = happybase.ConnectionPool(size=nb_threads,host='10.1.94.57',timeout=hbase_conn_timeout)


def get_create_table(table_name, conn, families={'info': dict()}):
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
            print "[get_create_table: info] table {} does not exist (yet): {}{}".format(table_name, type(inst),
                                                                                        inst)
            conn.create_table(table_name, families)
            table = conn.table(table_name)
            print "[get_create_table: info] created table {}".format(table_name)
            return table

def copy_batch(tab_in_name, start_row, batch_size, tab_out_name, tab_out_families):
    try:
        batch_start = time.time()
        batch, count, new_start_row = read_batch(tab_in_name, start_row, batch_size)
        write_batch(batch, tab_out_name, tab_out_families)
        tel = time.time()-batch_start
        print "Batch of {} rows from row {} copied in: {}s.".format(len(batch), start_row, tel)
        return count,new_start_row
    except Exception as inst:
        print "Copy batch failed. {}".format(inst)
        return None,None

def read_batch(tab_in_name, start_row, batch_size):
    batch = []
    new_start_row = start_row
    with pool.connection() as connection:
        tab_in = connection.table(tab_in_name)
        for one_row in tab_in.scan(row_start=start_row):
            if len(batch) < batch_size:
                batch.append(one_row)
                new_start_row = one_row[0]
            else:
                return batch,len(batch),new_start_row
        # If scan ends before filling batch, return partial final batch
        return batch,len(batch),new_start_row

def write_batch(batch, tab_out_name, tab_out_families):
    with pool.connection() as conn:
        tab_out = get_create_table(tab_out_name, conn, tab_out_families)
        batch_write = tab_out.batch()
        for row in batch:
            batch_write.put(row[0],row[1])
        batch_write.send()

def subsample_table(opts):
    start_time = time.time()
    copy_count = 0
    start_row = opts.start_row
    if opts.output_table_families:
        tab_out_families = dict()
        for family in opts.output_table_families.split(','):
            tab_out_families[family] = dict()
    while copy_count < opts.rows_count:
        if copy_count + opts.batch_size <= opts.rows_count:
            tmp_batch_size = opts.batch_size
        else:
            tmp_batch_size = opts.rows_count-copy_count
        count, new_start_row = copy_batch(opts.input_table, start_row, tmp_batch_size, opts.output_table, tab_out_families)
        if count is None:
            print "Did not copy any rows. Leaving."
            break
        else:
            if count == tmp_batch_size: # full batch
                copy_count += count
                start_row = new_start_row
            else: # incomplete last batch
                copy_count += count
                start_row = new_start_row
                print "Only managed to get {} out of {} rows requested.".format(copy_count, opts.rows_count)
                break
    tel = time.time() - start_time
    if copy_count > 0:
        print_msg = "Copied {} rows total, out of {} requested. Average time per row is: {}. Total time is: {}."
        print print_msg.format(copy_count, opts.rows_count, tel/copy_count, tel)
    

if __name__ == '__main__':

    # get options
    parser = ArgumentParser()
    parser.add_argument("-i", "--input_table", dest="input_table", required=True)
    parser.add_argument("-o", "--output_table", dest="output_table", required=True)
    parser.add_argument("-f", "--output_table_families", dest="output_table_families", required=False, default=None)
    parser.add_argument("-s", "--start_row", dest="start_row", default=None)
    parser.add_argument("-b", "--batch_size", dest="batch_size", type=int, default=100)
    parser.add_argument("-r", "--rows_count", dest="rows_count", type=int, default=1000)

    opts = parser.parse_args()

    subsample_table(opts)
