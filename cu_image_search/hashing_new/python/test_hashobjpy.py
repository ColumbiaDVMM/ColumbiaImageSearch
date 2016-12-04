from _hasher_obj_py import *

if __name__ == "__main__":
    hasher = new_HasherObjectPy()
    HasherObjectPy_read_update_files(hasher)
    HasherObjectPy_load_hashcodes(hasher)
    HasherObjectPy_load_itq_model(hasher)
