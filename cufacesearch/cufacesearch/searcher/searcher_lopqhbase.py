import sys
import time
import lmdb
import numpy as np
from datetime import datetime

from generic_searcher import GenericSearcher
# Beware: the loading function to use could depend on the featurizer type...
from ..featurizer.featsio import load_face_features
from ..indexer.hbase_indexer_minimal import column_list_sha1s, update_str_processed
from ..common.error import full_trace_error

START_HDFS = '/user/'

default_prefix = "SEARCHLOPQ_"

class SearcherLOPQHBase(GenericSearcher):

  def __init__(self, global_conf_in, prefix=default_prefix):
    # number of processors to use for parallel computation of codes
    self.num_procs = 8 # could be read from configuration
    self.model_params = None
    self.nb_train_pca = 100000
    self.last_refresh = datetime.now()
    self.last_full_refresh = datetime.now()
    self.last_indexed_update = None
    self.pca_model_str = None
    # making LOPQSearcherLMDB the default LOPQSearcher
    self.lopq_searcher = "LOPQSearcherLMDB"
    super(SearcherLOPQHBase, self).__init__(global_conf_in, prefix)

  def get_model_params(self):
    V = self.get_required_param('lopq_V')
    M = self.get_required_param('lopq_M')
    subq = self.get_required_param('lopq_subq')
    # we could use that for a more fine grained model naming...
    self.model_params = {'V': V, 'M': M, 'subq': subq}
    if self.model_type == "lopq_pca":
      # Number of dimensions to keep after PCA
      pca = self.get_required_param('lopq_pcadims')
      self.model_params['pca'] = pca
      nb_train_pca = self.get_required_param('nb_train_pca')
      self.nb_train_pca = nb_train_pca
    lopq_searcher = self.get_param('lopq_searcher')
    if lopq_searcher:
      self.lopq_searcher = lopq_searcher

  def build_pca_model_str(self):
    # Use feature type, self.nb_train_pca and pca_dims
    if self.pca_model_str is None:
      # We could add some additional info, like model parameters, number of samples used for training...
      self.pca_model_str = self.build_extr_str() + "_pca" + str(self.model_params['pca']) + "_train" + str(self.nb_train_pca)
    return self.pca_model_str

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
      self.save_feat_env = lmdb.open('./lmdb_feats_' + self.build_model_str(), map_size=1024 * 1000000 * 128,
                                     writemap=True, map_async=True, max_dbs=2)

      # Train and save model in save_path folder
      lopq_model = self.train_index()
      # TODO: we could build a more unique model identifier (using sha1/md5 of model parameters? using date of training?)
      #  that would also mean we should list from the storer and guess (based on date of creation) the correct model above...
      self.storer.save(self.build_model_str(), lopq_model)


    # Setup searcher with LOPQ model
    if lopq_model:
      # LOPQSearcherLMDB is now the default, as it makes the index more persistent and more easily usable with multiple processes.
      if self.lopq_searcher == "LOPQSearcherLMDB":
        from lopq.search import LOPQSearcherLMDB
        # TODO: should we get path from a parameter? and/or add model_str to it?
        # self.searcher = LOPQSearcherLMDB(lopq_model, lmdb_path='./lmdb_index/', id_lambda=str)
        # self.updates_env = lmdb.open('./lmdb_updates/', map_size=1024 * 1000000 * 1, writemap=True, map_async=True, max_dbs=1)
        self.searcher = LOPQSearcherLMDB(lopq_model, lmdb_path='./lmdb_index_'+self.build_model_str(), id_lambda=str)
        self.updates_env = lmdb.open('./lmdb_updates_'+self.build_model_str(), map_size=1024 * 1000000 * 1,
                                     writemap=True, map_async=True, max_dbs=1)
        self.updates_index_db = self.updates_env.open_db("updates")
      elif self.lopq_searcher == "LOPQSearcher":
        from lopq.search import LOPQSearcher
        self.searcher = LOPQSearcher(lopq_model)
      else:
        raise ValueError("Unknown 'lopq_searcher' type: {}".format(self.lopq_searcher))
    # NB: an empty lopq_model would make sense only if we just want to detect...

  def get_feats_from_lmbd(self, feats_db, nb_features, dtype):
    nb_saved_feats = self.get_nb_saved_feats(feats_db)
    nb_feats_to_read = min(nb_saved_feats, nb_features)
    feats = None
    if nb_feats_to_read > 0:
      with self.save_feat_env.begin(db=feats_db, write=False) as txn:
        with txn.cursor() as cursor:
          if cursor.first():
            first_item = cursor.item()
            first_feat = np.frombuffer(first_item[1], dtype=dtype)
            feats = np.zeros((nb_feats_to_read, first_feat.shape[0]))
            print "[get_feats_from_lmbd] Filling up features matrix: {}".format(feats.shape)
            sys.stdout.flush()
            for i, item in enumerate(cursor.iternext()):
              if i >= nb_feats_to_read:
                break
              feats[i, :] = np.frombuffer(item[1], dtype=dtype)
    return feats

  def save_feats_to_lmbd(self, feats_db, samples_ids, np_features, max_feats=0):
    with self.save_feat_env.begin(db=feats_db, write=True) as txn:
      for i, sid in enumerate(samples_ids):
        txn.put(bytes(sid), np_features[i, :].tobytes())
        nb_feats = txn.stat()['entries']
        if max_feats and nb_feats >= max_feats:
          return nb_feats
    return nb_feats

  def get_nb_saved_feats(self, feats_db):
    with self.save_feat_env.begin(db=feats_db, write=False) as txn:
      return txn.stat()['entries']

  def get_train_features(self, nb_features, lopq_pca_model=None):
    train_features = []
    #nb_saved_feats = 0
    # Save training features to disk, if we ever want to train with different model configurations as
    # gathering features can take a while
    # TODO: current pickle system likely to create out of memory error, would need to stream saving/loading or save to LMDB...
    #if self.save_train_features:
    if lopq_pca_model:
      feats_db = self.save_feat_env.open_db("feats_pca")
      dtype = np.float32
    else:
      feats_db = self.save_feat_env.open_db("feats")
      from ..featurizer.featsio import get_feat_dtype
      dtype = get_feat_dtype(self.featurizer_type)
    nb_saved_feats = self.get_nb_saved_feats(feats_db)

    if nb_saved_feats < nb_features:
      print "[{}: log] Gathering {} training samples...".format(self.pp, nb_features)
      sys.stdout.flush()
      start_date = "1970-01-01"
      done = False
      # Accumulate until we have enough features
      while not done:
        # TODO: this has been changed to a generator, need to be tested
        for batch_updates in self.indexer.get_updates_from_date(start_date=start_date, extr_type=self.build_extr_str()):
          for update in batch_updates:
            try:
              # We could check that update has been processed... but if it haven't we won't get features anyway...
              update_id = update[0]
              list_sha1s = update[1][column_list_sha1s]
              samples_ids, features = self.indexer.get_features_from_sha1s(list_sha1s.split(','), self.build_extr_str())
              if features:
                # Apply PCA to features here to save memory
                if lopq_pca_model:
                  np_features = lopq_pca_model.apply_PCA(np.asarray(features))
                else:
                  np_features = np.asarray(features)
                print "[{}: log] Got features {} from update {}".format(self.pp, np_features.shape, update_id)
                # just appending like this does not account for duplicates...
                #train_features.extend(np_features)
                nb_saved_feats = self.save_feats_to_lmbd(feats_db, samples_ids, np_features)
              else:
                print "[{}: log] Did not get features from update {}".format(self.pp, update_id)
              if nb_saved_feats >= nb_features:
                done = True
                break
            except Exception as inst:
              from cufacesearch.common.error import full_trace_error
              err_msg = "[{}: error] Failed to get features from update {}: {} {}".format(self.pp, update_id, type(inst), inst)
              full_trace_error(err_msg)
              sys.stdout.flush()
          else:
            print "[{}: log] Got {} training samples so far...".format(self.pp, nb_saved_feats)
            sys.stdout.flush()
          if done:
            break
        else:
          # Wait for new updates...
          print "[{}: log] Waiting for new updates...".format(self.pp)
          time.sleep(600)

    return self.get_feats_from_lmbd(feats_db, nb_features, dtype)

  def train_index(self):

      if self.model_type == "lopq":
        train_np = self.get_train_features(self.nb_train)
        print "Got train features array with shape: {}".format(train_np.shape)
        nb_train_feats = train_np.shape[0]
        sys.stdout.flush()

        if nb_train_feats >= self.nb_train:
          from lopq.model import LOPQModel
          # we could have default values for those parameters and/or heuristic to estimate them based on data count...
          lopq_model = LOPQModel(V=self.model_params['V'], M=self.model_params['M'],
                                 subquantizer_clusters=self.model_params['subq'])
          # we could have separate training/indexing features
          msg = "[{}.train_model: info] Starting local training of 'lopq' model with parameters {} using {} features."
          print msg.format(self.pp, self.model_params, nb_train_feats)
          start_train = time.time()
          # specify a n_init < 10 (default value) to speed-up training?
          lopq_model.fit(train_np, verbose=True)
          # save model
          self.storer.save(self.build_model_str(), lopq_model)
          print "[{}.train_model: info] Trained lopq model in {}s.".format(self.pp, time.time() - start_train)
          return lopq_model
        else:
          msg = "[{}.train_model: error] Could not train model, not enough training samples."
          print msg.format(self.pp)

      elif self.model_type == "lopq_pca":
        # lopq_pca training.
        from lopq.model import LOPQModelPCA
        # we could have default values for those parameters and/or heuristic to estimate them based on data count...
        lopq_model = LOPQModelPCA(V=self.model_params['V'], M=self.model_params['M'],
                                  subquantizer_clusters=self.model_params['subq'], renorm=True)
        # pca loading/training first
        pca_model = self.storer.load(self.build_pca_model_str())
        if pca_model is None:
          train_np = self.get_train_features(self.nb_train_pca)
          msg = "[{}.train_model: info] Starting training of PCA model, keeping {} dimensions from features {}."
          print msg.format(self.pp, self.model_params['pca'], train_np.shape)
          sys.stdout.flush()
          start_train_pca = time.time()
          lopq_model.fit_pca(train_np, pca_dims=self.model_params['pca'])
          print "[{}.train_model: info] Trained pca model in {}s.".format(self.pp, time.time() - start_train_pca)
          del train_np
          self.storer.save(self.build_pca_model_str(), {"P": lopq_model.pca_P, "mu":lopq_model.pca_mu})
        else:
          lopq_model.pca_P = pca_model["P"]
          lopq_model.pca_mu = pca_model["mu"]
        # train model
        train_np = self.get_train_features(self.nb_train, lopq_pca_model=lopq_model)
        msg = "[{}.train_model: info] Starting local training of 'lopq_pca' model with parameters {} using features {}"
        print msg.format(self.pp, self.model_params, train_np.shape)
        sys.stdout.flush()
        start_train = time.time()
        # specify a n_init < 10 (default value) to speed-up training?
        lopq_model.fit(train_np, verbose=True, apply_pca=False, train_pca=False)
        # TODO: we could evaluate model based on reconstruction of some randomly sampled features?
        # save model
        self.storer.save(self.build_model_str(), lopq_model)
        print "[{}.train_model: info] Trained lopq model in {}s.".format(self.pp, time.time() - start_train)
        sys.stdout.flush()
        return lopq_model
        #err_msg = "[{}.train_model: error] Local training of 'lopq_pca' model not yet implemented."
        #raise NotImplementedError(err_msg.format(self.pp))
      else:
        raise ValueError("[{}.train_model: error] Unknown 'lopq' type {}.".format(self.pp, self.model_type))
      #print train_features_path, os.path.exists(train_features_path), lopq_params

    # TODO: should we try to evaluate index by pushing train_features to a temporary searcher
    #    - compute exhaustive search for some randomly selected samples
    #    - analyze retrieval performance of approximate search?
    # technically we could even explore different configurations...


  def compute_codes(self, det_ids, data, codes_path=None):
    # Compute codes for each update batch and save them
    from lopq.utils import compute_codes_parallel
    msg = "[{}.compute_codes: info] Computing codes for {} {}s."
    print msg.format(self.pp, len(det_ids), self.input_type)

    # That keeps the ordering intact, but output is a chain
    codes = compute_codes_parallel(data, self.searcher.model, self.num_procs)

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

  def add_update(self, update_id):
    if self.lopq_searcher == "LOPQSearcherLMDB":
      # TODO: Should we use another LMDB to store updates indexed?
      with self.updates_env.begin(db=self.updates_index_db, write=True) as txn:
        txn.put(bytes(update_id), bytes(datetime.now()))
    else:
      self.indexed_updates.add(update_id)
    self.last_indexed_update = update_id

  def is_update_indexed(self, update_id):
    if self.lopq_searcher == "LOPQSearcherLMDB":
      with self.updates_env.begin(db=self.updates_index_db, write=False) as txn:
        found_update = txn.get(bytes(update_id))
        if found_update:
          return True
        else:
          return False
    else:
      return update_id in self.indexed_updates

  def get_latest_update_suffix(self):
    if self.last_indexed_update is None:
      if self.lopq_searcher == "LOPQSearcherLMDB":
        # Try to get in from DB
        with self.updates_env.begin(db=self.updates_index_db, write=False) as txn:
          with txn.cursor() as cursor:
            if cursor.last():
              item = cursor.item()
              self.last_indexed_update = item[0]
              suffix = '_'.join(self.last_indexed_update.split('_')[6:])
            else:  # Would happen on empty db?
              suffix = "1970-01-01"
      else:
        suffix = "1970-01-01"
    else:
      suffix = '_'.join(self.last_indexed_update.split('_')[6:])
    return suffix

  def load_codes(self, full_refresh=False):
    # Calling this method can also perfom an update of the index
    if not self.searcher:
      print "[{}.load_codes: info] Not loading codes as searcher is not initialized.".format(self.pp)
      return

    start_load = time.time()
    total_compute_time = 0

    # if self.searcher.nb_indexed == 0:
    #   # We should try to load a concatenation of all unique codes that also contains a list of the corresponding updates...
    #   # fill codes and self.indexed_updates
    #   self.load_all_codes()
    # TODO: try to get date of last update
    start_date = "1970-01-01"
    if not full_refresh:
      start_date = self.get_latest_update_suffix()

    # Get all updates ids for the extraction type
    for batch_updates in self.indexer.get_updates_from_date(start_date=start_date, extr_type=self.build_extr_str()):
      for update in batch_updates:
        #print "[{}: log] batch length: {}, update length: {}".format(self.pp, len(batch_updates),len(update))
        update_id = update[0]
        if self.is_update_indexed(update_id):
          print "[{}: log] Skipping update {} already indexed.".format(self.pp, update_id)
        else:
          if "info:"+update_str_processed in update[1]:
            print "[{}: log] Looking for codes of update {}".format(self.pp, update_id)
            # Get this update codes
            codes_string = self.build_codes_string(update_id)
            try:
              # Check for precomputed codes
              codes_dict = self.storer.load(codes_string)
              if codes_dict is None:
                raise ValueError('Could not load codes: {}'.format(codes_string))
            except Exception as inst:
              # Update codes not available
              if self.verbose > 1:
                print "[{}: log] Update {} codes could not be loaded: {}".format(self.pp, update_id, inst)
              # Compute codes for update not yet processed and save them
              start_compute = time.time()
              # Get detections (if any) and features...
              list_sha1s = update[1][column_list_sha1s]
              # Double check that this gets properly features of detections
              samples_ids, features = self.indexer.get_features_from_sha1s(list_sha1s.split(','), self.build_extr_str())
              codes_dict = self.compute_codes(samples_ids, features, codes_string)
              update_compute_time = time.time() - start_compute
              total_compute_time += update_compute_time
              if self.verbose > 0:
                print "[{}: log] Update {} codes computation done in {}s".format(self.pp, update_id, update_compute_time)

            # Use new method add_codes_from_dict of searcher
            self.searcher.add_codes_from_dict(codes_dict)
            # TODO: indexed_updates should be made persistent too, and add indexing time
            self.add_update(update_id)

          else:
            print "[{}: log] Skipping unprocessed update {}".format(self.pp, update_id)
        # TODO: we could check that update processing time was older than indexing time, otherwise that means that
        #    the update has been reprocessed and should be re-indexed.


    # # We should save all_codes
    # self.save_all_codes()

    total_load = time.time() - start_load
    self.last_refresh = datetime.now()
    #if self.verbose > 0:

    print "[{}: log] Total udpates computation time is: {}s".format(self.pp, total_compute_time)
    print "[{}: log] Total udpates loading time is: {}s".format(self.pp, total_load)

  # def load_all_codes(self):
  #   # load self.indexed_updates, self.searcher.index and self.searcher.nb_indexed
  #   # NOT for LOPQSearcherLMDB
  #   pass
  #
  # def save_all_codes(self):
  #   # we should save self.indexed_updates, self.searcher.index and self.searcher.nb_indexed
  #   # self.searcher.index could be big, how to save without memory issue...
  #   # NOT for LOPQSearcherLMDB
  #   pass

  def search_from_feats(self, dets, feats, options_dict=dict()):
    import time
    start_search = time.time()
    all_sim_images = []
    all_sim_dets = []
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
    quota = min(100 * max_returned, 1000)

    #print dets
    if self.detector is not None:
      # query for each feature
      for i in range(len(dets)):

        sim_images = []
        sim_dets = []
        sim_score = []

        for j in range(len(dets[i][1])):
          results = []
          if "detect_only" not in options_dict or not options_dict["detect_only"]:
            if self.searcher:
              # Normalize feature first as it is how it is done during extraction...
              norm_feat = np.linalg.norm(feats[i][j])
              normed_feat = np.squeeze(feats[i][j] / norm_feat)
              results, visited = self.searcher.search(normed_feat, quota=quota, limit=max_returned, with_dists=True)
              res_msg = "[{}.search_from_feats: log] got {} results by visiting {} cells, first one is: {}"
              print res_msg.format(self.pp, len(results), visited, results[0])

          # If reranking, get features from hbase for detections using res.id
          #   we could also already get 's3_url' to avoid a second call to HBase later...
          if self.reranking:
            res_list_sha1s = [str(x.id).split('_')[0] for x in results]
            res_samples_ids, res_features = self.indexer.get_features_from_sha1s(res_list_sha1s, self.build_extr_str())

          tmp_img_sim = []
          tmp_dets_sim_ids = []
          tmp_dets_sim_score = []
          for ires, res in enumerate(results):
            dist = res.dist
            if (filter_near_dup and dist <= near_dup_th) or not filter_near_dup:
              if not max_returned or (max_returned and ires < max_returned ):
                tmp_dets_sim_ids.append(res.id)
                # here id would be face_id that we could build as sha1_facebbox?
                tmp_img_sim.append(str(res.id).split('_')[0])
                # if reranking compute actual distance
                if self.reranking:
                  try:
                    pos = res_samples_ids.index(res.id)
                    dist = np.linalg.norm(normed_feat - res_features[pos])
                    print "{}: res_features[{}] approx. dist: {}, rerank dist: {}".format(res.id, pos, res.dist, dist)
                  except Exception as inst:
                    print "Could not compute reranking distance for sample {}, error {} {}".format(res.id, type(inst), inst)
                tmp_dets_sim_score.append(dist)

          # If reranking, we need to reorder
          if self.reranking:
            sids = np.argsort(tmp_dets_sim_score, axis=0)
            rerank_img_sim = []
            rerank_dets_sim_ids = []
            rerank_dets_sim_score = []
            for si in sids:
              rerank_img_sim.append(tmp_img_sim[si])
              rerank_dets_sim_ids.append(tmp_dets_sim_ids[si])
              rerank_dets_sim_score.append(tmp_dets_sim_score[si])
            tmp_img_sim = rerank_img_sim
            tmp_dets_sim_ids = rerank_dets_sim_ids
            tmp_dets_sim_score = rerank_dets_sim_score

          #print tmp_img_sim
          if tmp_img_sim:
            rows = self.indexer.get_columns_from_sha1_rows(tmp_img_sim, self.needed_output_columns)
            # rows should contain id, s3_url of images
            #print rows
            sim_images.append(rows)
            sim_dets.append(tmp_dets_sim_ids)
            sim_score.append(tmp_dets_sim_score)
          else:
            sim_images.append([])
            sim_dets.append([])
            sim_score.append([])

        all_sim_images.append(sim_images)
        all_sim_dets.append(sim_dets)
        all_sim_score.append(sim_score)
    else:
      # No detection
      results = []
      sim_images = []
      sim_score = []

      for i in range(len(feats)):
        if self.searcher:
          # Normalize feature first as it is how it is done during extraction...
          norm_feat = np.linalg.norm(feats[i])
          normed_feat = np.squeeze(feats[i] / norm_feat)
          results, visited = self.searcher.search(normed_feat, quota=quota, limit=max_returned, with_dists=True)
          res_msg = "[{}.search_from_feats: log] got {} results by visiting {} cells, first one is: {}"
          print res_msg.format(self.pp, len(results), visited, results[0])

        # Reranking, get features from hbase for detections using res.id
        if self.reranking:
          res_list_sha1s = [str(x.id) for x in results]
          res_samples_ids, res_features = self.indexer.get_features_from_sha1s(res_list_sha1s, self.build_extr_str())

        tmp_img_sim = []
        tmp_sim_score = []
        for ires, res in enumerate(results):
          dist = res.dist
          if (filter_near_dup and dist <= near_dup_th) or not filter_near_dup:
            if not max_returned or (max_returned and ires < max_returned):
              tmp_img_sim.append(str(res.id))
              if self.reranking:
                # If reranking compute actual distance
                try:
                  pos = res_samples_ids.index(res.id)
                  tmp_sim_score.append(np.linalg.norm(normed_feat - res_features[pos]))
                except Exception as inst:
                  print "Could not compute reranked distance for sample {}, error {} {}".format(res.id, type(inst), inst)
                  tmp_sim_score.append(dist)
              else:
                tmp_sim_score.append(dist)

        # If reranking, we need to reorder
        if self.reranking:
          sids = np.argsort(tmp_sim_score, axis=0)
          rerank_img_sim = []
          rerank_sim_score = []
          for si in sids:
            rerank_img_sim.append(tmp_img_sim[si])
            rerank_sim_score.append(tmp_sim_score[si])
          tmp_img_sim = rerank_img_sim
          tmp_sim_score = rerank_sim_score

        if tmp_img_sim:
          rows = self.indexer.get_columns_from_sha1_rows(tmp_img_sim, self.needed_output_columns)
          # rows should contain id, s3_url of images
          # print rows
          sim_images.append(rows)
          sim_score.append(tmp_sim_score)
        else:
          sim_images.append([])
          sim_score.append([])

      all_sim_images.append(sim_images)
      all_sim_dets.append([])
      all_sim_score.append(sim_score)

    search_time = time.time() - start_search
    print 'Search performed in {:0.3}s.'.format(search_time)

    # format output
    #print "all_sim_images",all_sim_images
    #print "all_sim_dets",all_sim_dets
    #print "all_sim_score",all_sim_score
    return self.do.format_output(dets, all_sim_images, all_sim_dets, all_sim_score, options_dict, self.input_type)
