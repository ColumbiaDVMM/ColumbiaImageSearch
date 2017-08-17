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
    self.num_procs = 6

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
      # 'LOPQModelPCA' could be the type of the model loaded from pickle file
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
    train_features_path = self.get_param('train_features_path')
    lopq_params = self.get_param('lopq_params')
    if train_features_path and lopq_params and os.path.exists(train_features_path):
      if self.model_type == "lopq":
        import json
        import time
        import pickle
        from lopq.model import LOPQModel
        jlp = json.loads(lopq_params)
        # we could have default values for those parameters and/or heuristic to estimate them based on data count...
        lopq_model = LOPQModel(V=jlp['V'], M=jlp['M'], subquantizer_clusters=jlp['subq'])
        # we could have separate training/indexing features
        face_ids, data = load_face_features(train_features_path, verbose=True)
        msg = "[{}.train_model: info] Starting local training of 'lopq' model with parameters {} using features from {}."
        print msg.format(self.pp, lopq_params, train_features_path)
        start_train = time.time()
        # specify a n_init<10 (default value) to speed-up training?
        lopq_model.fit(data, verbose=True)
        # save model
        out_dir = os.path.dirname(lopq_model_path)
        try:
          os.makedirs(out_dir)
        except:
          pass
        pickle.dump(lopq_model, open(lopq_model_path, 'wb'))
        print "[{}.train_model: info] Trained lopq model in {}s.".format(self.pp, time.time() - start_train)
        return lopq_model
      elif self.model_type == "lopq_pca":
        err_msg = "[{}.train_model: error] Local training of 'lopq_pca' model not yet implemented."
        raise NotImplementedError(err_msg.format(self.pp))
      else:
        raise ValueError("[{}.train_model: error] Unknown 'lopq' type {}.".format(self.pp, self.model_type))
    else:
      msg = "[{}.train_model: error] Could not train 'lopq' model. "
      msg += "Have you specified 'train_features_path' (and path exists?) and 'lopq_params' in config?"
      print msg.format(self.pp)
      #print train_features_path, os.path.exists(train_features_path), lopq_params

  def compute_codes(self, face_ids, data, codes_path, model=None):
    from lopq.utils import compute_codes_parallel
    msg = "[{}.compute_codes: info] Computing codes for {} faces."
    print msg.format(self.pp, len(face_ids))
    if model is None:
      model = self.searcher.model
    # that keeps the ordering intact, but output is a chain
    codes = compute_codes_parallel(data, model, self.num_procs)
    out_dir = os.path.dirname(codes_path)
    try:
      os.makedirs(out_dir)
    except:
      pass
    # to be saved as tsv, e.g. each line is face_id\tcode
    # allow for appending, beware to start from empty file...
    with open(codes_path, 'at') as outf:
      for i, code in enumerate(codes):
        outf.write("{}\t{}\n".format(face_ids[i], [code.coarse, code.fine]))

  def load_codes(self):
    # TODO: how to deal with updates?

    if self.codes_path:
      if not self.searcher:
        print "[{}.load_codes: info] Not loading codes as searcher is not initialized.".format(self.pp)
        return
      import time
      start_load = time.time()
      if self.codes_path.startswith(START_HDFS):
        self.searcher.add_codes_from_hdfs(self.codes_path)
        print "[{}.load_codes: info] Loaded codes in {}s.".format(self.pp, time.time() - start_load)
      else:
        if os.path.exists(self.codes_path):
          self.searcher.add_codes_from_local(self.codes_path)
          print "[{}.load_codes: info] Loaded codes in {}s.".format(self.pp, time.time() - start_load)
        else:
          # Try to compute codes from features files?
          index_features_path = self.get_param('index_features_path')
          import glob
          if glob.glob(index_features_path):
            print "[{}.load_codes: info] Computing codes from 'features_path': {}".format(self.pp, index_features_path)
            for ifn in glob.glob(index_features_path):
              for root, dirs, files in os.walk(ifn):
                for basename in files:
                  # TODO: allow features files not starting with 'part-'
                  if basename.startswith('part-'):
                    face_ids, data = load_face_features(os.path.join(root, basename), verbose=True)
                    self.compute_codes(face_ids, data, self.codes_path)
            self.searcher.add_codes_from_local(self.codes_path)
            print "[{}.load_codes: info] Loaded codes in {}s.".format(self.pp, time.time() - start_load)
          else:
            print "[{}.load_codes: info] 'codes_path' and 'index_features_path' do no exists on disk. Nothing to index!".format(self.pp)
    else:
      print "[{}.load_codes: info] 'codes_path' is not defined or empty in configuration file.".format(self.pp)

  def search_from_feats(self, dets, feats, options_dict=dict()):
    import time
    start_search = time.time()
    all_sim_images = []
    all_sim_faces = []
    all_sim_score = []
    # check what is the near duplicate config
    filter_near_dup = False
    if (self.near_dup and "near_dup" not in options_dict) or ("near_dup" in options_dict and options_dict["near_dup"]):
      filter_near_dup = True
      if "near_dup_th" in options_dict:
        near_dup_th = options_dict["near_dup_th"]
      else:
        near_dup_th = self.near_dup_th

    max_returned = self.sim_limit
    if "max_returned" in options_dict:
      max_returned = options_dict["max_returned"]

    #print dets

    # query for each feature
    for i in range(len(dets)):

      sim_images = []
      sim_faces = []
      sim_score = []

      for j in range(len(dets[i][1])):
        results = []
        if "detect_only" not in options_dict or not options_dict["detect_only"]:
          if self.searcher:
            if self.model_type == "lopq_pca":
              feat = np.squeeze(self.searcher.model.apply_PCA(feats[i][j]))
            else:
              feat = np.squeeze(feats[i][j])
            norm_feat = np.linalg.norm(feat)
            normed_feat = feat / norm_feat
            # print "[SearcherLOPQHBase.search_from_feats: log] pca_projected_feat.shape: {}".format(pca_projected_feat.shape)
            # format of results is a list of namedtuples as: namedtuple('Result', ['id', 'code', 'dist'])
            #results, visited = self.searcher.search(feat, quota=self.quota, limit=self.sim_limit, with_dists=True)
            results, visited = self.searcher.search(normed_feat, quota=2 * max_returned, limit=max_returned, with_dists=True)
            #print "[{}.search_from_feats: log] got {} results, first one is: {}".format(self.pp, len(results), results[0])
        tmp_img_sim = []
        tmp_face_sim_ids = []
        tmp_face_sim_score = []
        for ires,res in enumerate(results):
          if (filter_near_dup and res.dist <= near_dup_th) or not filter_near_dup:
            if not max_returned or (max_returned and ires < max_returned ):
              tmp_face_sim_ids.append(res.id)
              # here id would be face_id that we could build as sha1_facebbox?
              tmp_img_sim.append(str(res.id).split('_')[0])
              tmp_face_sim_score.append(res.dist)

        #print tmp_img_sim

        if tmp_img_sim:
          rows = self.indexer.get_columns_from_sha1_rows(tmp_img_sim, self.needed_output_columns)
          # rows should contain id, s3_url of images
          #print rows
          sim_images.append(rows)
          sim_faces.append(tmp_face_sim_ids)
          sim_score.append(tmp_face_sim_score)
        else:
          sim_images.append([])
          sim_faces.append([])
          sim_score.append([])

      all_sim_images.append(sim_images)
      all_sim_faces.append(sim_faces)
      all_sim_score.append(sim_score)

    search_time = time.time() - start_search
    print 'Search performed in {:0.3}s.'.format(search_time)

    # format output
    return self.do.format_output(dets, all_sim_images, all_sim_faces, all_sim_score, options_dict)
