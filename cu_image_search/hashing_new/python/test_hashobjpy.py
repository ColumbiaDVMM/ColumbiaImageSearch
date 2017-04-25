import _hasher_obj_py as hop
import sys

# TODO:
# - Get paths from conf file
# - Add one sample .dat file to test this

if __name__ == "__main__":
    hasher = hop.new_HasherObjectPy()
    hop.HasherObjectPy_set_base_updatepath(hasher, "/home/ubuntu/memex/update/indexing/")
    up_path = hop.HasherObjectPy_get_base_updatepath(hasher)
    print(up_path)
    hop.HasherObjectPy_set_base_modelpath(hasher, "/home/ubuntu/memex/data/")
    status = hop.HasherObjectPy_initialize(hasher)
    if status != 0:
        print("Hasher was not able to initialize")
        sys.exit(-1)
    featurefilename = "/home/ubuntu/memex/DeepSentiBank_memex/cu_image_search/www/1480830128.81.dat"
    featurefilename = "../../../scripts/index_update_8525658676137_5411457.dat"
    hop.HasherObjectPy_set_query_feats_from_disk(hasher, featurefilename)
    hop.HasherObjectPy_set_outputfile(hasher, featurefilename[:-4])
    hop.HasherObjectPy_find_knn(hasher)

    # this is actually not working yet.
    #hop.HasherObjectPy_set_query_feats_from_disk(hasher, featurefilename)
    #out_res = hop.HasherObjectPy_find_knn_nodiskout(hasher)

    # cleanup
    delete_HasherObjectPy(hasher)
