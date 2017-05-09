### This seems to work fine...
# Is the real script not working because consumer takes hours to process an update?

import os
import sys
import time
import datetime

# parallel
from multiprocessing import Queue
from multiprocessing import Process
import random

nb_workers = 2
time_sleep = 10
consumer_random_multiplier = 600
max_update_count = 10
queue_timeout = 5


def random_sleep(multiplier=20):
    time.sleep(int(random.random()*multiplier))


def get_now():
    return datetime.datetime.now().strftime("%Y-%m-%d:%H.%M.%S")

# should we try/except main loop of producer, consumer and finalizer?
def end_producer(queueIn):
    print "[producer-pid({}): log] ending producer at {}".format(os.getpid(), get_now())
    for i in range(nb_workers):
        # sentinel value, one for each worker
        queueIn.put((None, None, None))


def producer(global_conf_file, queueIn, queueProducer):
    print "[producer-pid({}): log] Started a producer worker at {}".format(os.getpid(), get_now())
    sys.stdout.flush()
    random_sleep()
    print "[producer-pid({}): log] Producer worker ready at {}".format(os.getpid(), get_now())
    queueProducer.put("Producer ready")
    update_count = 0
    while True:
        try:
            start_get_batch = time.time()
            random_sleep()
            if update_count<=max_update_count:
                update_id = "update_{}".format(update_count)
                str_list_sha1s = ["sha1_{}".format(x) for x in range(10)]
                valid_sha1s = str_list_sha1s
                update_count += 1
                print "[producer-pid({}): log] Got batch in {}s at {}".format(os.getpid(), time.time() - start_get_batch, get_now())
                sys.stdout.flush()
            else:
                print "[producer-pid({}): log] No more update to process.".format(os.getpid())
                return end_producer(queueIn)
            start_precomp = time.time()
            print "[producer-pid({}): log] Pushing update {} at {}.".format(os.getpid(), update_id, get_now())
            sys.stdout.flush()
            queueIn.put((update_id, valid_sha1s, start_precomp))
            print "[producer-pid({}): log] Pushed update {} to queueIn at {}.".format(os.getpid(), update_id, get_now())
            sys.stdout.flush()
        except Exception as inst:
            print "[producer-pid({}): error] Error at {}. Leaving. Error was: {}".format(os.getpid(), get_now(), inst)
            return end_producer(queueIn)


def end_consumer(queueIn, queueOut):
    print "[consumer-pid({}): log] ending consumer at {}".format(os.getpid(), get_now())
    #queueIn.task_done()
    queueOut.put((None, None, None, None, None, None))


def consumer(global_conf_file, queueIn, queueOut, queueConsumer):
    print "[consumer-pid({}): log] Started a consumer worker at {}".format(os.getpid(), get_now())
    sys.stdout.flush()
    random_sleep()
    print "[consumer-pid({}): log] Consumer worker ready at {}".format(os.getpid(), get_now())
    queueConsumer.put("Consumer ready")
    sys.stdout.flush()
    while True:
        try:
            ## reads from queueIn
            print "[consumer-pid({}): log] Consumer worker waiting for update at {}".format(os.getpid(), get_now())
            sys.stdout.flush()
            #update_id, valid_sha1s, start_precomp = queueIn.get(True, queue_timeout)
            update_id, valid_sha1s, start_precomp = queueIn.get(block=True, timeout=queue_timeout)
            if update_id is None:
                # declare worker ended
                print "[consumer-pid({}): log] Consumer worker ending at {}".format(os.getpid(), get_now())
                return end_consumer(queueIn, queueOut)
            ## Search
            print "[consumer-pid({}): log] Consumer worker computing similarities for {} valid sha1s of update {} at {}".format(os.getpid(), len(valid_sha1s), update_id, get_now())
            sys.stdout.flush()
            start_search = time.time()
            random_sleep(consumer_random_multiplier)
            # Fake work
            simname = update_id+"_sim.txt"
            corrupted = []
            random_sleep(consumer_random_multiplier)
            elapsed_search = time.time() - start_search
            print "[consumer-pid({}): log] Consumer worker processed update {} at {}. Search performed in {}s.".format(os.getpid(), update_id, get_now(), elapsed_search)
            sys.stdout.flush()
            ## push to queueOut
            start_push = time.time()
            queueOut.put((update_id, simname, valid_sha1s, corrupted, start_precomp, elapsed_search))
            print "[consumer-pid({}): log] Consumer worker pushed update {} to queueOut in {}s at {}.".format(os.getpid(), update_id, time.time()-start_push, get_now())
            sys.stdout.flush()
        except Exception as inst:
            print "[consumer-pid({}): error] Consumer worker caught error at {}. Error was {}".format(os.getpid(), get_now(), inst)
            #return end_consumer(queueIn, queueOut)


def end_finalizer(queueOut, queueFinalizer):
    print "[finalizer-pid({}): log] ending finalizer at {}".format(os.getpid(), get_now())
    queueFinalizer.put("Finalizer ended")


def finalizer(global_conf_file, queueOut, queueFinalizer):
    print "[finalizer-pid({}): log] Started a finalizer worker at {}".format(os.getpid(), get_now())
    sys.stdout.flush()
    random_sleep()
    print "[finalizer-pid({}): log] Finalizer worker ready at {}".format(os.getpid(), get_now())
    queueFinalizer.put("Finalizer ready")
    count_workers_ended = 0
    while True:
        try:
            ## Read from queueOut
            print "[finalizer-pid({}): log] Finalizer worker waiting for an update at {}".format(os.getpid(), get_now())
            sys.stdout.flush()
            # This seems to block (or not getting updates info) even if there are items that have been pushed to the queueOut??
            # timeout seems to be 10s even if called with queue_timeout=600...
            #update_id, simname, valid_sha1s, corrupted, start_precomp, elapsed_search = queueOut.get(True, queue_timeout)
            update_id, simname, valid_sha1s, corrupted, start_precomp, elapsed_search = queueOut.get(block=True, timeout=queue_timeout)
            if update_id is None:
                count_workers_ended += 1
                print "[finalizer-pid({}): log] {} consumer workers ended out of {} at {}.".format(os.getpid(), count_workers_ended, nb_workers, get_now())
                #queueOut.task_done()
                if count_workers_ended == nb_workers:
                    # fully done
                    print "[finalizer-pid({}): log] All consumer workers ended at {}. Leaving.".format(os.getpid(), get_now())
                    return end_finalizer(queueOut, queueFinalizer)
                continue
            print "[finalizer-pid({}): log] Finalizer worker got update {} from queueOut to finalize at {}".format(os.getpid(), update_id, get_now())
            sys.stdout.flush()

            ## Fake finalizing
            random_sleep()

            print "[finalizer-pid({}): log] Finalize update {} at {} in {}s total.".format(os.getpid(), update_id, get_now(), time.time() - start_precomp)
            sys.stdout.flush()
            
        except Exception as inst:
            #[finalizer: error] Caught error at 2017-04-14:04.29.23. Leaving. Error was: list index out of range
            print "[finalizer-pid({}): error] Caught error at {}. Error {} was: {}".format(os.getpid(), get_now(), type(inst), inst)
            # now we catch timeout too, so we are no longer leaving...
            #return end_finalizer(queueOut, queueFinalizer)




def parallel_precompute(global_conf_file=None):
    # Define queues
    queueIn = Queue(nb_workers+2)
    queueOut = Queue(nb_workers+8)
    queueProducer = Queue()
    queueFinalizer = Queue()
    queueConsumer = Queue(nb_workers)

    # Start finalizer
    t = Process(target=finalizer, args=(global_conf_file, queueOut, queueFinalizer))
    t.daemon = True
    t.start()
    # Start consumers
    for i in range(nb_workers):
        t = Process(target=consumer, args=(global_conf_file, queueIn, queueOut, queueConsumer))
        t.daemon = True
        t.start()
    # Start producer
    t = Process(target=producer, args=(global_conf_file, queueIn, queueProducer))
    t.daemon = True
    t.start()

    # Wait for everything to be started properly
    producerOK = queueProducer.get()
    finalizerOK = queueFinalizer.get()
    for i in range(nb_workers):
        consumerOK = queueConsumer.get()
    print "[parallel_precompute: log] All workers are ready."
    sys.stdout.flush()
    # Wait for everything to be finished
    finalizerEnded = queueFinalizer.get()
    print "[parallel_precompute: log] Done at {}".format(get_now())
    return
    


if __name__ == "__main__":
    
    """ Simulate the similar images precomputation to debug queue issue.
    """
    
    while True:
        parallel_precompute()
        print "[precompute_similar_images_parallel: log] Nothing to compute. Sleeping for {}s.".format(time_sleep)
        sys.stdout.flush()
        time.sleep(time_sleep)
    
    
    
