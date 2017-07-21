import os
import numpy as np

from generic_searcher import GenericSearcher
# Beware: the loading function to use could depend on the featurizer type...
from ..featurizer.featsio import load_face_features

START_HDFS = '/user/'

class SearcherLOPQHBase(GenericSearcher):

  def set_pp(self):
    self.pp = "SearcherLOPQHBase"
    self.model_type = "lopq"
    # number of processors to use for parallel computation of codes
    self.num_procs = 4

  def init_searcher(self):
    """ Initialize LOPQ model and searcher from `global_conf` value.
    """
    import pickle
    # Get model type from conf file
    lopq_model_type = self.get_param('lopq')
    lopq_model = None
    # Deal with potential different LOPQ model types
    if not lopq_model_type:
      raise ValueError("[{}: error] 'lopq' is not defined in configuration file.".format(self.pp))
    elif lopq_model_type == "lopq" or lopq_model_type == "lopq_pca":
      self.model_type = lopq_model_type
      # this is from our modified LOPQ package...
      # https://github.com/ColumbiaDVMM/ColumbiaImageSearch/tree/master/workflows/build-lopq-index/lopq/python
      # 'LOPQModelPCA' will be the type of the model loaded from pickle file
      from lopq.model import LOPQModel, LOPQModelPCA
      lopq_model_path = self.get_param('lopqmodel')
      if lopq_model_path:
        # deal with HDFS path
        if lopq_model_path.startswith(START_HDFS):
          from lopq.utils import copy_from_hdfs
          import shutil
          filename = copy_from_hdfs(lopq_model_path)
          lopq_model = pickle.load(filename)
          try:
            shutil.rmtree(os.path.dirname(filename))
          except:
            pass
        else:
          if os.path.exists(lopq_model_path):
            # local path in config
            lopq_model = pickle.load(open(lopq_model_path, "rb"))
          else:
            print "[{}: error] Could not find lopq model at: {}".format(self.pp, lopq_model_path)
            # Train and save model at lopq_model_path
            lopq_model = self.train_model(lopq_model_path)
      else:
        print "[{}: info] Emtpy lopq model path".format(self.pp)

    else:
      raise ValueError("[{}: error] Unknown 'lopq' type {}.".format(self.pp, lopq_model_type))

    # Setup searcher with LOPQ model
    if lopq_model:
      from lopq.search import LOPQSearcher
      self.searcher = LOPQSearcher(lopq_model)
      # NB: an empty lopq_model would make sense only if we just want to detect...

  def train_model(self, lopq_model_path):
    features_path = self.get_param('features_path')
    lopq_params = self.get_param('lopq_params')
    if os.path.exists(features_path) and lopq_params and features_path and self.codes_path:
      if self.model_type == "lopq":
        import json
        import time
        import pickle
        from lopq.model import LOPQModel
        jlp = json.loads(lopq_params)
        # we could have default values for those parameters and/or heuristic to estimate them based on data count...
        lopq_model = LOPQModel(V=jlp['V'], M=jlp['M'], subquantizer_clusters=jlp['subq'])
        # we could have separate training/indexing features
        face_ids, data = load_face_features(features_path)
        msg = "[{}.train_model: info] Starting local training of 'lopq' model with parameters {} using features from {}."
        print msg.format(self.pp, lopq_params, features_path)
        start_train = time.time()
        lopq_model.fit(data, verbose=True)
        # save model
        out_dir = os.path.dirname(lopq_model_path)
        try:
          os.makedirs(out_dir)
        except:
          pass
        pickle.dump(lopq_model, lopq_model_path)
        print "[{}.train_model: info] Trained lopq model in {}s.".format(self.pp, time.time() - start_train)
        # Compute codes too?
        print "[{}.train_model: info] Computing codes.".format(self.pp)
        self.compute_codes(face_ids, data, self.codes_path, model=lopq_model)
        print "[{}.train_model: info] Saved codes at: {}".format(self.pp, self.codes_path)
      elif self.model_type == "lopq_pca":
        err_msg = "[{}.train_model: error] Local training of 'lopq_pca' model not yet implemented."
        raise NotImplementedError(err_msg.format(self.pp))
      else:
        raise ValueError("[{}.train_model: error] Unknown 'lopq' type {}.".format(self.pp, self.model_type))
    else:
      msg = "[{}.train_model: error] Could not train 'lopq' model. "
      msg += "Have you specified 'features_path' (and path exists?), 'codes_path' and 'lopq_params' in config?"
      print msg.format(self.pp)
      print features_path, os.path.exists(features_path), self.codes_path, lopq_params

  def compute_codes(self, face_ids, data, codes_path, model=None):
    from lopq.utils import compute_codes_parallel
    # Beware: does that keep the ordering intact?
    if model is None:
      model = self.searcher.model
    codes = compute_codes_parallel(data, model, self.num_procs)
    out_dir = os.path.dirname(codes_path)
    try:
      os.makedirs(out_dir)
    except:
      pass
    # to be saved as tsv, e.g. each line is face_id\tcode
    with open(codes_path, 'wt') as outf:
      for i, face_id in face_ids:
        outf.write("{}\t{}\n".format(face_id, codes[i]))

  def load_codes(self):
    # TODO: how to deal with updates?
    if self.codes_path:
      if not self.searcher:
        print "[{}.load_codes: info] Not loading codes as searcher is not initialized.".format(self.pp)
        return
      try:
        if self.codes_path.startswith(START_HDFS):
          self.searcher.add_codes_from_hdfs(self.codes_path)
        else:
          self.searcher.add_codes_from_local(self.codes_path)
      except Exception as inst:
        print "[{}.load_codes: info] Could not load codes from 'codes_path': {}".format(self.pp, self.codes_path)
        # Try to compute codes from features files?
        features_path = self.get_param('features_path')
        if os.path.exists(features_path):
          print "[{}.load_codes: info] Computing codes from 'features_path': {}".format(self.pp, features_path)
          face_ids, data = load_face_features(features_path)
          self.compute_codes(face_ids, data, self.codes_path)
          self.searcher.add_codes_from_local(self.codes_path)
    else:
      print "[{}.load_codes: info] 'codes_path' is not defined or empty in configuration file.".format(self.pp)

  def search_from_feats(self, dets, feats, options_dict=dict()):
    sim_images = []
    sim_faces = []
    sim_score = []
    # check what is the near duplicate config
    filter_near_dup = False
    if (self.near_dup and "near_dup" not in options_dict) or ("near_dup" in options_dict and options_dict["near_dup"]):
      filter_near_dup = True
      if "near_dup_th" in options_dict:
        near_dup_th = options_dict["near_dup_th"]
      else:
        near_dup_th = self.near_dup_th
    # query for each feature
    for i in range(len(dets)):

      for j in range(len(dets[i][1])):
        results = []
        if "detect_only" not in options_dict or not options_dict["detect_only"]:
          norm_feat = np.linalg.norm(feats[i][j])
          if self.searcher:
            if self.model_type == "lopq_pca":
              feat = np.squeeze(self.searcher.model.apply_PCA(feats[i][j] / norm_feat))
            else:
              feat = np.squeeze(feats[i][j] / norm_feat)
            # print "[SearcherLOPQHBase.search_from_feats: log] pca_projected_feat.shape: {}".format(pca_projected_feat.shape)
            # format of results is a list of namedtuples as: namedtuple('Result', ['id', 'code', 'dist'])
            results, visited = self.searcher.search(feat,
                                                         quota=self.quota,
                                                         limit=self.sim_limit,
                                                         with_dists=True)
            print "[{}.search_from_feats: log] got {} results, first one is: {}".format(self.pp, len(results), results[0])
        tmp_img_sim = []
        tmp_face_sim_ids = []
        tmp_face_sim_score = []
        for res in results:
          if (filter_near_dup and res.dist <= near_dup_th) or not filter_near_dup:
            tmp_face_sim_ids.append(res.id)
            # here id would be face_id that we could build as sha1_facebbox?
            tmp_img_sim.append(str(res.id).split('_')[0])
            tmp_face_sim_score.append(res.dist)

        if tmp_img_sim:
          rows = self.indexer.get_columns_from_sha1_rows(tmp_img_sim, self.needed_output_columns)
          # rows should contain id, s3_url of images
          sim_images.append(rows)
          sim_faces.append(tmp_face_sim_ids)
          sim_score.append(tmp_face_sim_score)
        else:
          sim_images.append([])
          sim_faces.append([])
          sim_score.append([])

    # format output
    return self.do.format_output(dets, sim_images, sim_faces, sim_score, options_dict)
