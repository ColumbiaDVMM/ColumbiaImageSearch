from _hasher_obj_py import *
import sys

if __name__ == "__main__":
    hasher = new_HasherObjectPy()
    # setters are not working?
    up_path = HasherObjectPy_get_base_updatepath(hasher)
    print(up_path)
    HasherObjectPy_set_base_updatepath(hasher, "/home/ubuntu/memex/update/indexing/")
    up_path = HasherObjectPy_get_base_updatepath(hasher)
    print(up_path)
    HasherObjectPy_set_base_modelpath(hasher, "/home/ubuntu/memex/data/")
    HasherObjectPy_set_paths(hasher)
    status = HasherObjectPy_read_update_files(hasher)
    if status != 0:
        print("Hasher was not able to read update")
	sys.exit(-1)
    HasherObjectPy_load_itq_model(hasher)
    HasherObjectPy_fill_data_nums_accum(hasher)
    HasherObjectPy_load_hashcodes(hasher)
