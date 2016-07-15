import sys
sys.path.append('../..')
import time
import base64
import numpy as np

from cu_image_search.indexer.hbase_indexer import HBaseIndexer
from cu_image_search.memex_tools.sha1_tools import get_SHA1_from_file
from cu_image_search.memex_tools.binary_file import read_binary_file, write_binary_file 

if __name__=="__main__":
    HBI = HBaseIndexer('../../conf/global_var_remotehbase.json')
    tab_sha1_name = 'escorts_images_sha1_infos_ext'    
    
    sha1_to_check = ['0000007031E3FA80C97940017253BEAB542EA334','000000A7CAD184BD6BC00DAF032E5F7C818C39D0','000000D29139258BD3716C94A68CFF54A8A7C033']
    res,ok_ids = HBI.get_precomp_from_sha1(sha1_to_check,["sentibank","hashcode"])
    print res[0]
    fn = "tmp_test_binary"
    write_binary_file(fn,[np.frombuffer(base64.b64decode(x)) for x in res[0]])
    feats_from_binary,ok_ids_binary = read_binary_file(fn,"sentibank",[0, 1, 2],4096*4,np.float32)
