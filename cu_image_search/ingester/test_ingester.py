from cdr_ingester import CDRIngester
from hbase_ingester import HBaseIngester

if __name__=="__main__":
    test_ingester = "hbase"
    if test_ingester=="cdr":
        conf_file="../../conf/global_var.json"
        CDRIn = CDRIngester(conf_file)
        CDRIn.set_start(0)
        CDRIn.set_batch_size(10)
        CDRIn.set_verbose(2)
        batch = CDRIn.get_batch()
        print batch
    if test_ingester=="hbase":
        conf_file="../../conf/global_var_remotehbase.json"
        HBIn = HBaseIngester(conf_file)
        HBIn.set_start('~')
        HBIn.set_batch_size(10)
        HBIn.set_verbose(2)
        batch = HBIn.get_batch()
        print batch
