# v1 has an issue with queueOut.
# Finalizer is not always getting the processed update, seems to hang for a long time.

import os
import sys
import time
import datetime
import happybase

# parallel
from multiprocessing import Queue
from multiprocessing import Process

sys.path.append('..')
import cu_image_search
from cu_image_search.search import searcher_hbaseremote

nb_workers = 1
time_sleep = 60
queue_timeout = 600
debug_sleep = 10
debug = True

producer_end_signal = (None, None, None)
consumer_end_signal = "consumer_ended"
finalizer_end_signal = "finalizer_ended"


def end_finalizer(queueFinalizer):
    print "[finalizer-pid({}): log] ending finalizer at {}".format(os.getpid(), get_now())
    queueFinalizer.put("Finalizer ended")


def finalizer(global_conf_file, queueOut, queueFinalizer):
    print "[finalizer-pid({}): log] Started a finalizer worker at {}".format(os.getpid(), get_now())
    sys.stdout.flush()
    import glob
    searcher_finalizer = searcher_hbaseremote.Searcher(global_conf_file)
    print "[finalizer-pid({}): log] Finalizer worker ready at {}".format(os.getpid(), get_now())
    sys.stdout.flush()
    queueFinalizer.put("Finalizer ready")
    count_workers_ended = 0
    sim_pattern = '*-sim_'+str(searcher_finalizer.ratio)+'.txt'
    while True:
        try:
            ## Read from queueOut
            print "[finalizer-pid({}): log] Finalizer worker waiting for an update at {}".format(os.getpid(), get_now())
            sys.stdout.flush()
            found_update = False

            ## Use glob to list of files that would match the simname pattern.
            list_simfiles = glob.glob(sim_pattern)

            for simname in list_simfiles:
                found_update = True
                start_finalize = time.time()

                # parse update_id
                update_id = simname.split('-')[0]
                
                print "[finalizer-pid({}): log] Finalizer worker found update {} to finalize at {}".format(os.getpid(), update_id, get_now())
                sys.stdout.flush()

                ## Check if update was not already finished by another finalizer?

                ## Push computed similarities
                
                # format for saving in HBase:
                # - batch_sim: should be a list of sha1 row key, dict of "s:similar_sha1": dist_value
                # - batch_mark_precomp_sim: should be a list of sha1 row key, dict of precomp_sim_column: True
                batch_sim, batch_mark_precomp_sim = format_batch_sim_v2(simname, searcher_finalizer)

                # push similarities to HBI_table_sim (escorts_images_similar_row_dev) using searcher.indexer.write_batch
                if batch_sim:
                    searcher_finalizer.indexer.write_batch(batch_sim, searcher_finalizer.indexer.table_sim_name)
                    # push to weekly update table for Amandeep to integrate in DIG
                    week, year = get_week_year()
                    weekly_sim_table_name = searcher_finalizer.indexer.table_sim_name+"_Y{}W{}".format(year, week)
                    print "[finalizer-pid({}): log] weekly table name: {}".format(os.getpid(), weekly_sim_table_name)
                    weekly_sim_table = searcher_finalizer.indexer.get_create_table(weekly_sim_table_name, families={'s': dict()})
                    searcher_finalizer.indexer.write_batch(batch_sim, weekly_sim_table_name)

                    ## Mark as done
                    # mark precomp_sim true in escorts_images_sha1_infos
                    searcher_finalizer.indexer.write_batch(batch_mark_precomp_sim, searcher_finalizer.indexer.table_sha1infos_name)
                    # mark updated has processed 
                    searcher_finalizer.indexer.write_batch([(update_id, {searcher_finalizer.indexer.precomp_end_marker: 'True'})],
                                                       searcher_finalizer.indexer.table_updateinfos_name)

                
                ## Cleanup
                try:
                    # remove simname 
                    os.remove(simname)
                    # remove features file
                    featfn = update_id+'.dat'
                    os.remove(featfn)
                except Exception as inst:
                    print "[finalizer-pid({}): error] Could not cleanup. Error was: {}".format(os.getpid(), inst)

                # We don't have start_precomp anymore
                #print "[finalizer-pid({}): log] Finalize update {} at {} in {}s total.".format(os.getpid(), update_id, get_now(), time.time() - start_precomp)
                print "[finalizer-pid({}): log] Finalized update {} at {} in {}s.".format(os.getpid(), update_id, get_now(), time.time() - start_finalize)
                sys.stdout.flush()
                # if debug:
                #     print "Sleeping for {}s.".format(debug_sleep)
                #     sys.stdout.flush()
                #     time.sleep(debug_sleep)
            
            # Check if consumers have ended
            try:
                end_signal = queueOut.get(block=False)
                if end_signal == consumer_end_signal:
                    count_workers_ended += 1
                    print "[finalizer-pid({}): log] {} consumer workers ended out of {} at {}.".format(os.getpid(), count_workers_ended, nb_workers, get_now())
                    if count_workers_ended == nb_workers:
                        # should we check for intermediate sim patterns to know if consumers are actually still running, or failed?
                        # sim_pattern = '*-sim.txt'
                        # fully done
                        print "[finalizer-pid({}): log] All consumer workers ended at {}. Leaving.".format(os.getpid(), get_now())
                        return end_finalizer(queueFinalizer)
                    continue
            except Exception as inst: #timeout
                pass

            # Sleep if no updates where found in this loop cycle?
            if not found_update:
                time.sleep(time_sleep)

        except Exception as inst:
            #[finalizer: error] Caught error at 2017-04-14:04.29.23. Leaving. Error was: list index out of range
            print "[finalizer-pid({}): error] Caught error at {}. Error {} was: {}".format(os.getpid(), get_now(), type(inst), inst)
            # now we catch timeout too, so we are no longer leaving...
            #return end_finalizer(queueOut, queueFinalizer)


def get_now():
    return datetime.datetime.now().strftime("%Y-%m-%d:%H.%M.%S")


def get_week(today=datetime.datetime.now()):
    return today.strftime("%W")


def get_year(today=datetime.datetime.now()):
    return today.strftime("%Y")


def get_week_year(today=datetime.datetime.now()):
    week = get_week(today)
    year = get_year(today)
    return week, year


def read_sim_precomp_v2(simname, searcher, nb_query=None):
    # intialization
    sim = []
    sim_score = []
    if simname is not None:
        # read similar images
        count = 0
        f = open(simname);
        for line in f:
            #sim_index.append([])
            nums = line.replace(' \n','').split(' ')
            #filter near duplicate here
            nums = searcher.filter_near_dup(nums, searcher.near_dup_th)
            #print nums
            onum = len(nums)/2
            n = onum
            #print n
            # this is not really possible since we are querying with DB images here.
            if onum==0: # no returned images, e.g. no near duplicate
                sim.append(())
                sim_score.append([])
                continue
            # get the sha1s of similar images
            sim_infos = [searcher.indexer.sha1_featid_mapping[int(i)] for i in nums[0:n]]
            # beware, need to make sure sim and sim_score are still aligned
            #print("[read_sim] got {} sim_infos from {} samples".format(len(sim_infos), n))
            sim.append(sim_infos)
            sim_score.append(nums[onum:onum+n])
            count = count + 1
            if nb_query and count == nb_query:
                break
        f.close()
    return sim, sim_score
    

def format_batch_sim_v2(simname, searcher):
    # format similarities for HBase output
    sim, sim_score = read_sim_precomp_v2(simname, searcher)
    print "[format_batch_sim_v2: log] {} has {} images with precomputed similarities.".format(simname, len(sim))
    sys.stdout.flush()
    # batch_sim: should be a list of sha1 row key, dict of all "s:similar_sha1": dist_value
    batch_sim = []
    # batch_mark_precomp_sim: should be a list of sha1 row key, dict of precomp_sim_column: True
    batch_mark_precomp_sim = []
    if sim:
        for i_img,list_sim in enumerate(sim):
            # query sha1
            sha1 = list_sim[0]
            list_score = sim_score[i_img]
            # if debug:
            #     print "[format_batch_sim_v2: log] {} is similar to: {}".format(sha1, list_sim)
            # to store query -> similar images
            sim_columns = dict()
            for i_sim,sim_img in enumerate(list_sim):
                sim_columns["s:"+str(sim_img)] = str(list_score[i_sim])
                # to store similar image -> query
                sim_reverse = dict()
                sim_reverse["s:"+sha1] = str(list_score[i_sim])
                batch_sim.append((str(sim_img), sim_reverse))
            sim_row = (sha1, sim_columns)
            batch_sim.append(sim_row)
            batch_mark_precomp_sim.append((sha1,{searcher.indexer.precomp_sim_column: 'True'}))
    # if debug:
    #     print "[format_batch_sim_v2: log] batch_sim: {}".format(batch_sim)
    #     print "[format_batch_sim_v2: log] batch_mark_precomp_sim: {}".format(batch_mark_precomp_sim)
    #     time.sleep(debug_sleep)
    return batch_sim, batch_mark_precomp_sim


def parallel_precompute(global_conf_file):
    # Define queues
    queueOut = Queue(1)
    queueFinalizer = Queue(1)
    

    # Start finalizer
    t = Process(target=finalizer, args=(global_conf_file, queueOut, queueFinalizer))
    t.daemon = True
    t.start()

    finalizerOK = queueFinalizer.get()
    print "[parallel_precompute: log] All workers are ready."
    sys.stdout.flush()
    # Fake end
    queueOut.put(consumer_end_signal)
    # Wait for everything to be finished
    finalizerEnded = queueFinalizer.get()
    print finalizerEnded
    return
    


if __name__ == "__main__":
    
    """ Run precompute similar images based on `conf_file` given as parameter
    """
    if len(sys.argv)<2:
        print "python precompute_similar_images_parallel.py conf_file"
        exit(-1)
    global_conf_file = sys.argv[1]
    
    while True:
        parallel_precompute(global_conf_file)
        print "[precompute_similar_images_parallel: log] Nothing to compute. Sleeping for {}s.".format(time_sleep)
        sys.stdout.flush()
        time.sleep(time_sleep)
    
    
    
