import os
import json
from argparse import ArgumentParser

if __name__ == "__main__":
  # Get config
  parser = ArgumentParser()
  parser.add_argument("-o", "--output_dir", dest="output_dir", required=True)
  options = parser.parse_args()

  # Initialization
  conf = dict()
  conf_name = os.environ['conf_name']
  search_prefix = "SEARCHLOPQ_"
  storer_prefix = "ST_"
  hbase_prefix = "HBI_"

  # Generic ingestion settings
  verbose = int(os.getenv('verbose', 0))
  conf[search_prefix + "verbose"] = verbose
  conf[search_prefix + 'get_pretrained_model'] = False

  conf[search_prefix + 'storer_prefix'] = storer_prefix
  conf[search_prefix + 'indexer_prefix'] = hbase_prefix
  conf[search_prefix + 'indexer_type'] = "hbase_indexer_minimal"
  storer_type = os.environ['storer']
  if storer_type == "local":
    conf[search_prefix + 'storer_type'] = storer_type
    conf[storer_prefix + 'base_path'] = os.getenv('storer_base_path', './data/index')
    conf[storer_prefix + 'verbose'] = verbose
  # TODO: deal with AWS

  # Extraction settings
  extr_type = os.environ['extr_type']
  if extr_type == "dlibface":
    # Extractions options
    featurizer_prefix = "DLIB_"
    conf[search_prefix + 'featurizer_prefix'] = featurizer_prefix
    conf[search_prefix + 'featurizer_type'] = "dlib"
    conf[search_prefix + 'detector_type'] = "dlib"
    conf[search_prefix + 'input_type'] = "face"
    # conf[featurizer_prefix + 'pred_path'] = "./data/models/shape_predictor_68_face_landmarks.dat"
    # conf[featurizer_prefix + 'rec_path'] = "./data/models/dlib_face_recognition_resnet_model_v1.dat"
    conf[featurizer_prefix + 'pred_path'] = "/data/models/shape_predictor_68_face_landmarks.dat"
    conf[featurizer_prefix + 'rec_path'] = "/data/models/dlib_face_recognition_resnet_model_v1.dat"
  elif extr_type == "sbpycaffeimg":
    featurizer_prefix = "SBPY_"
    conf[search_prefix + 'featurizer_prefix'] = featurizer_prefix
    conf[search_prefix + 'featurizer_type'] = "sbpycaffe"
    conf[search_prefix + 'detector_type'] = "full"
    conf[search_prefix + 'input_type'] = "image"
    #conf[featurizer_prefix + 'sbcaffe_path'] = "./data/models/caffe_sentibank_train_iter_250000"
    #conf[featurizer_prefix + 'imgmean_path'] = "./data/models/imagenet_mean.npy"
    conf[featurizer_prefix + 'sbcaffe_path'] = "/data/models/caffe_sentibank_train_iter_250000"
    conf[featurizer_prefix + 'imgmean_path'] = "/data/models/imagenet_mean.npy"
  else:
    raise ValueError("Unknown extraction type: {}".format(extr_type))

  # HBase settings
  conf[hbase_prefix + 'host'] =  os.environ['hbase_host']
  conf[hbase_prefix + 'table_sha1infos'] = os.environ['table_sha1infos']
  conf[hbase_prefix + 'table_updateinfos'] = os.environ['table_updateinfos']
  conf[hbase_prefix + 'batch_update_size'] = int(os.environ['batch_update_size'])
  conf[hbase_prefix + 'pool_thread'] = 1

  # Local input settings
  if os.environ['input_type'] == "local":
    conf[search_prefix + 'file_input'] = True

  # Search parameters
  conf[search_prefix + 'model_type'] = os.environ['model_type']
  conf[search_prefix + 'nb_train'] = int(os.environ['nb_train'])
  conf[search_prefix + 'lopq_V'] = int(os.environ['lopq_V'])
  conf[search_prefix + 'lopq_M'] = int(os.environ['lopq_M'])
  conf[search_prefix + 'lopq_subq'] = int(os.environ['lopq_subq'])
  conf[search_prefix + 'reranking'] = os.getenv('reranking', True)
  if conf[search_prefix + 'model_type'] == "lopq_pca":
    conf[search_prefix + 'nb_train_pca'] = int(os.environ['nb_train_pca'])
    conf[search_prefix + 'lopq_pcadims'] = int(os.environ['lopq_pcadims'])

  if not os.path.exists(options.output_dir):
    os.mkdir(options.output_dir)

  json.dump(conf, open(os.path.join(options.output_dir,'conf_search_'+conf_name+'.json'),'wt'), sort_keys=True, indent=4)

