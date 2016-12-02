from _hasher_obj_py import *   

if __name__ == "__main__":
    hasher = HasherObjectPy()
    hasher.read_update_files()
    hasher.load_hashcodes()
    hasher.load_itq_model()
