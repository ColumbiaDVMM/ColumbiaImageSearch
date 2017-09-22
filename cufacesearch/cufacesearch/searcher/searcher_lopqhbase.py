import sys
import time
import numpy as np

from generic_searcher import GenericSearcher
# Beware: the loading function to use could depend on the featurizer type...
from ..featurizer.featsio import load_face_features
from ..indexer.hbase_indexer_minimal import column_list_sha1s
from ..common.error import full_trace_error

START_HDFS = '/user/'

default_prefix = "SEARCHLOPQ_"

class SearcherLOPQHBase(GenericSearcher):

  def __init__(self, global_conf_in, prefix=default_prefix):
    # number of processors to use for parallel computation of codes
    self.num_procs = 6 # could be read from configuration
    super(SearcherLOPQHBase, self).__init__(global_conf_in, prefix)


  def set_pp(self):
    self.pp = "SearcherLOPQHBase"

  def init_searcher(self):
    """ Initialize LOPQ model and searcher from `global_conf` value.
    """
    try:
      # Try to load pretrained model from storer
      lopq_model = self.storer.load(self.build_model_str())
      if lopq_model is None:
        raise ValueError("Could not load model from storer.")
    except Exception as inst:
      if type(inst) != ValueError:
        full_trace_error(inst)
      print "[{}: log] Looks like model was not trained yet ({})".format(self.pp, inst)

      # # this is from our modified LOPQ package...
      # # https://github.com/ColumbiaDVMM/ColumbiaImageSearch/tree/master/workflows/build-lopq-index/lopq/python
      # # 'LOPQModelPCA' could be the type of the model loaded from pickle file
      # from lopq.model import LOPQModel, LOPQModelPCA

      # Train and save model in save_path folder
      lopq_model = self.train_index()
      # TODO: we could build a more unique model identifier (using sha1/md5 of model parameters? using date of training?)
      #  that would also mean we should list from the storer and guess (based on date of creation) the correct model above...
      self.storer.save(self.build_model_str(), lopq_model)


    # Setup searcher with LOPQ model
    if lopq_model:
      from lopq.search import LOPQSearcher
      self.searcher = LOPQSearcher(lopq_model)
      # NB: an empty lopq_model would make sense only if we just want to detect...

  def get_train_features(self):
    print "[{}: log] Gathering {} training samples...".format(self.pp, self.nb_train)
    # TODO: test that...
    train_features = []
    start_date = "1970-01-01"
    while len(train_features) < self.nb_train:
      updates = self.indexer.get_updates_from_date(start_date=start_date, extr_type=self.build_extr_str())
      # Accumulate until we have enough features
      for update in updates:
        try:
          update_id = update[0]
          print "[{}: log] Getting features from update {}".format(self.pp, update_id)
          start_date = "_".join(update_id.split("_")[-2:-1])
          list_sha1s = update[1][column_list_sha1s]
          _, features = self.indexer.get_features_from_sha1s(list_sha1s.split(','), self.build_extr_str())
          train_features.extend(features)
          if len(train_features) >= self.nb_train:
            break
        except Exception as inst:
          print "[{}: error] Failed to get features from update {}: {} {}".format(self.pp, update_id, type(inst), inst)
          sys.stdout.flush()
      else:
        print "[{}: log] Got {} training samples so far...".format(self.pp, len(train_features))
        # Wait for new updates...
        time.sleep(600)
    return train_features

  def train_index(self):
    # TODO: test training
    train_features = self.get_train_features()
    train_np = np.asarray(train_features)
    self.storer.save("train_features", train_np)
    #print train_np.shape
    if len(train_features) >= self.nb_train:
      if self.model_type == "lopq":
        from lopq.model import LOPQModel
        # should get that from config file actually,
        # and we could use that for a more fine grained model naming...
        V = self.get_required_param('lopq_V')
        M = self.get_required_param('lopq_M')
        subq = self.get_required_param('lopq_subq')
        jlp = {'V': V, 'M': M, 'subq':subq}
        # we could have default values for those parameters and/or heuristic to estimate them based on data count...
        lopq_model = LOPQModel(V=jlp['V'], M=jlp['M'], subquantizer_clusters=jlp['subq'])
        # we could have separate training/indexing features
        msg = "[{}.train_model: info] Starting local training of 'lopq' model with parameters {} using {} features."
        print msg.format(self.pp, jlp, len(train_features))
        start_train = time.time()
        # specify a n_init < 10 (default value) to speed-up training?
        lopq_model.fit(train_np, verbose=True)
        # save model
        self.storer.save(self.build_model_str(), lopq_model)
        print "[{}.train_model: info] Trained lopq model in {}s.".format(self.pp, time.time() - start_train)
        return lopq_model
      elif self.model_type == "lopq_pca":
        # TODO: implement lopq_pca training.
        err_msg = "[{}.train_model: error] Local training of 'lopq_pca' model not yet implemented."
        raise NotImplementedError(err_msg.format(self.pp))
      else:
        raise ValueError("[{}.train_model: error] Unknown 'lopq' type {}.".format(self.pp, self.model_type))
    else:
      msg = "[{}.train_model: error] Could not train model, not enough training samples."
      print msg.format(self.pp)
      #print train_features_path, os.path.exists(train_features_path), lopq_params
    # TODO: should we try to evaluate index by pushing train_features to a temporary searcher
    #    - compute exhaustive search for some randomly selected samples
    #    - analyze retrieval performance of approximate search?
    # technically we could even explore different configurations...


  def compute_codes(self, det_ids, data, codes_path=None, model=None):
    # Compute codes for each update batch and save them
    from lopq.utils import compute_codes_parallel
    msg = "[{}.compute_codes: info] Computing codes for {} {}s."
    print msg.format(self.pp, len(det_ids), self.input_type)

    if model is None:
      model = self.searcher.model

    # That keeps the ordering intact, but output is a chain
    codes = compute_codes_parallel(data, model, self.num_procs)

    # Build dict output
    codes_dict = dict()
    for i, code in enumerate(codes):
      codes_dict[det_ids[i]] = [code.coarse, code.fine]

    # Save
    if codes_path:
      self.storer.save(codes_path, codes_dict)

    return codes_dict

    # Deprecated direct save to disk as tsv
    # out_dir = os.path.dirname(codes_path)
    # try:
    #   os.makedirs(out_dir)
    # except:
    #   pass
    # # to be saved as tsv, e.g. each line is face_id\tcode
    # # allow for appending, beware to start from empty file...
    # with open(codes_path, 'at') as outf:
    #   for i, code in enumerate(codes):
    #     outf.write("{}\t{}\n".format(face_ids[i], [code.coarse, code.fine]))



  def load_codes(self):
    # Calling this method can also perfom an update of the index
    if not self.searcher:
      print "[{}.load_codes: info] Not loading codes as searcher is not initialized.".format(self.pp)
      return

    start_load = time.time()
    total_compute_time = 0
    # Get all updates ids for the extraction type
    updates = self.indexer.get_updates_from_date(start_date="1970-01-01", extr_type=self.build_extr_str())
    for update in updates:
      update_id = update[0]
      if update_id not in self.indexed_updates:

        # Get this update codes
        codes_string = self.build_codes_string(update_id)
        try:
          # Check for precomputed codes
          codes_dict = self.storer.load(codes_string)
          if codes_dict is None:
            raise ValueError('Could not load codes: {}'.format(codes_string))
        except Exception as inst:
          # Compute codes for update not yet processed and save them
          start_compute = time.time()
          # Update codes not available
          if self.verbose > 1:
            print "[{}: log] Update {} codes could not be loaded: {}".format(self.pp, update_id, inst)
          # Get detections (if any) and features...
          list_sha1s = update[1][column_list_sha1s]
          samples_ids, features = self.indexer.get_features_from_sha1s(list_sha1s.split(','), self.build_extr_str())
          codes_dict = self.compute_codes(samples_ids, features, codes_string)
          update_compute_time = time.time() - start_compute
          total_compute_time += update_compute_time
          if self.verbose > 0:
            print "[{}: log] Update {} codes computation done in {}s".format(self.pp, update_id, update_compute_time)

        # Use new method add_codes_from_dict of searcher
        self.searcher.add_codes_from_dict(codes_dict)
        self.indexed_updates.add(update_id)

    total_load = time.time() - start_load
    if self.verbose > 0:
      print "[{}: log] Total udpates computation time is: {}s".format(self.pp, total_compute_time)
      print "[{}: log] Total udpates loading time is: {}s".format(self.pp, total_load)


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
    # this should be set with a parameter either in conf or options_dict too.
    # should we use self.quota here? and potentially overwrite from options_dict
    quota = 2 * max_returned

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
            results, visited = self.searcher.search(normed_feat, quota=quota, limit=max_returned, with_dists=True)
            #print "[{}.search_from_feats: log] got {} results, first one is: {}".format(self.pp, len(results), results[0])

        # TODO: If reranking, get features from hbase for detections using res.id
        #   we could also already get 's3_url' to avoid a second call to HBase later...

        tmp_img_sim = []
        tmp_face_sim_ids = []
        tmp_face_sim_score = []
        for ires, res in enumerate(results):
          dist = res.dist
          # TODO: if reranking compute actual distance
          if (filter_near_dup and dist <= near_dup_th) or not filter_near_dup:
            if not max_returned or (max_returned and ires < max_returned ):
              tmp_face_sim_ids.append(res.id)
              # here id would be face_id that we could build as sha1_facebbox?
              tmp_img_sim.append(str(res.id).split('_')[0])
              tmp_face_sim_score.append(dist)
          # TODO: rerank using tmp_face_sim_score

        #print tmp_img_sim
        # If
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
    print all_sim_images
    print all_sim_faces
    print all_sim_score
    return self.do.format_output(dets, all_sim_images, all_sim_faces, all_sim_score, options_dict, self.input_type)
