import os
import json
from argparse import ArgumentParser

if __name__ == "__main__":
  # Get config
  parser = ArgumentParser()
  parser.add_argument("-o", "--output_dir", dest="output_dir", required=True)
  options = parser.parse_args()

  # General environment variables
  # - conf_name (required)
  # - extr_type (required)
  # - input_type (required)
  # Hbase related environment variables
  # - hbase_host (required)
  # - table_sha1infos (required)
  # - table_updateinfos (required)
  # - batch_update_size (required)
  # Search related environment variables
  # - model_type (required)
  # - nb_train (required)
  # - nb_min_train (optional)
  # - lopq_V (required)
  # - lopq_M (required)
  # - lopq_subq (required)
  # - reranking  (optional, default: true)
  # If model_type is lopq_pca:
  # - nb_train_pca (required)
  # - nb_min_train_pca (optional)
  # - lopq_pcadims (required)
  # TODO: add storer infos.
  # TODO: report this list in the docs.
  # Make sure the docker-compose propagate all these variables down, so we can generate conf files in docker...

  # Initialization
  conf = dict()
  conf_name = os.environ['conf_name']
  search_prefix = "SEARCHLOPQ_"
  storer_prefix = "ST_"
  hbase_prefix = "HBI_"

  # Generic ingestion settings
  verbose = int(os.getenv('verbose', 0))
  conf[search_prefix + "verbose"] = verbose

  conf[search_prefix + 'storer_prefix'] = storer_prefix
  conf[search_prefix + 'indexer_prefix'] = hbase_prefix
  conf[hbase_prefix + 'verbose'] = verbose
  conf[storer_prefix + 'verbose'] = verbose
  # This overwrites what is set in 'searcher_lopqhbase.py'...
  #conf[search_prefix + 'get_pretrained_model'] = False
  # We only have this type of indexer for now...
  conf[search_prefix + 'indexer_type'] = "hbase_indexer_minimal"
  storer_type = os.environ['storer']
  if storer_type == "local":
    conf[search_prefix + 'storer_type'] = storer_type
    conf[storer_prefix + 'base_path'] = os.getenv('storer_base_path', '/data/index')
  if storer_type == "s3":
    conf[search_prefix + 'storer_type'] = storer_type
    # A file 'credentials' should exist in docker at /home/ubuntu/.aws/
    conf[storer_prefix + 'aws_profile'] = os.environ['aws_profile']
    conf[storer_prefix + 'bucket_name'] = os.environ['aws_bucket_name']

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
  # TODO: add sbcmdline for legacy data
  elif extr_type == "sbcmdlineimg":
    featurizer_prefix = "SBCMD_"
    conf[search_prefix + 'featurizer_prefix'] = featurizer_prefix
    conf[search_prefix + 'featurizer_type'] = "sbcmdline"
    conf[search_prefix + 'detector_type'] = "full"
    conf[featurizer_prefix + 'sbcaffe_path'] = "/data/models/caffe_sentibank_train_iter_250000"
    # What should it be?
    conf[featurizer_prefix + 'caffe_exec_path'] = "/home/ubuntu/caffe_cpu/build/tools/extract_nfeatures"
  else:
    raise ValueError("Unknown extraction type: {}".format(extr_type))

  # HBase settings
  conf[hbase_prefix + 'host'] =  os.environ['hbase_host']
  conf[hbase_prefix + 'table_sha1infos'] = os.environ['table_sha1infos']
  conf[hbase_prefix + 'table_updateinfos'] = os.environ['table_updateinfos']
  conf[hbase_prefix + 'batch_update_size'] = int(os.environ['batch_update_size'])
  # TODO: should we expose that parameter
  conf[hbase_prefix + 'pool_thread'] = 1

  # Local input settings
  # NB: confusing names between that input_type and the extraction input_type i.e. 'face' or 'image'
  if os.environ['input_type'] == "local":
    conf[search_prefix + 'file_input'] = True

  # Search parameters
  conf[search_prefix + 'model_type'] = os.environ['model_type']
  conf[search_prefix + 'nb_train'] = int(os.environ['nb_train'])
  conf[search_prefix + 'nb_min_train'] = int(os.getenv('nb_min_train', conf[search_prefix + 'nb_train']))
  conf[search_prefix + 'lopq_V'] = int(os.environ['lopq_V'])
  conf[search_prefix + 'lopq_M'] = int(os.environ['lopq_M'])
  conf[search_prefix + 'lopq_subq'] = int(os.environ['lopq_subq'])
  conf[search_prefix + 'reranking'] = os.getenv('reranking', True)
  if conf[search_prefix + 'model_type'] == "lopq_pca":
    conf[search_prefix + 'nb_train_pca'] = int(os.environ['nb_train_pca'])
    conf[search_prefix + 'nb_min_train_pca'] = int(os.getenv('nb_min_train_pca', conf[search_prefix + 'nb_train_pca']))
    conf[search_prefix + 'lopq_pcadims'] = int(os.environ['lopq_pcadims'])

  if not os.path.exists(options.output_dir):
    os.mkdir(options.output_dir)

  outpath = os.path.join(options.output_dir,'conf_search_'+conf_name+'.json')
  json.dump(conf, open(outpath,'wt'), sort_keys=True, indent=4)
  print("Saved conf at {}: {}".format(outpath, json.dumps(conf)))

