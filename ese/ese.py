#!/usr/bin/env python
from __future__ import unicode_literals
import argparse
from multiprocessing import Process, Queue, Value
from uuid import uuid4
from time import sleep
import traceback
from datetime import datetime

from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import bulk, scan

import logging
logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

__author__ = "kheechin"

def get_elasticsearch(hostname, port):
    return Elasticsearch(hosts=['http://%s:%s/' % (hostname, port)], max_retries=10, timeout=60, retry_on_timeout=True, connection_class=RequestsHttpConnection)

def src_worker(args, dest_queue, MAGIC_STRING):
    # print args.src_host, args.src_port, args.src_index, args.src_batch_size
    src_es_instance = get_elasticsearch(args.src_host, args.src_port)
    if args.query is None or len(args.query) == 0:
        use_query = {"query":{"match_all":{}}}
    else:
        use_query = args.query
    try:
        scroll = scan(src_es_instance, query=use_query, index=args.src_index, scroll='60m', size=args.src_batch_size)
        for i, res in enumerate(scroll):
            # if i == 0: log.info(res)
            if i % 10000 == 0:
                log.info("[src_worker] Processed %s" % i)
            dest_queue.put(res)
        log.info("[src_worker] Total processed %s" % i)
    except:
        log.error(traceback.format_exc())
    finally:
        for i in xrange(args.dest_concurrency):
            dest_queue.put(MAGIC_STRING)

def dest_worker(args, dest_queue, MAGIC_STRING, DEST_COUNTER):
    # print args.dest_host, args.dest_port, args.dest_index, args.dest_batch_size
    dest_es_instance = get_elasticsearch(args.dest_host, args.dest_port)
    BATCH_LIST = []
    for datum in iter(dest_queue.get, MAGIC_STRING):
        datum["_index"] = args.dest_index
        BATCH_LIST.append(datum)
        if len(BATCH_LIST) % args.dest_batch_size == 0:
            bulk(dest_es_instance, BATCH_LIST, chunk_size=args.dest_batch_size, refresh=False, stats_only=True)
            BATCH_LIST = []
        with DEST_COUNTER.get_lock():
            if DEST_COUNTER.value % 10000 == 0:
                log.info("[dest_worker] Processed %s" % (DEST_COUNTER.value, ))
            DEST_COUNTER.value += 1
    if len(BATCH_LIST) > 0:
        bulk(dest_es_instance, BATCH_LIST, chunk_size=args.dest_batch_size, refresh=False, stats_only=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-host", action="store", default="127.0.0.1", type=unicode, help="Source host [default: %(default)s]")
    parser.add_argument("--src-port", action="store", default=9200, help="Source port [default: %(default)s]")
    parser.add_argument("--src-index", action="store", default="", type=unicode, help="Source index")
    parser.add_argument("--src-batch-size", action="store", type=int, default=5000, help="Source query batchsize [default: %(default)s]")

    parser.add_argument("--dest-host", action="store", default="127.0.0.1", type=unicode, help="Destination host [default: %(default)s]")
    parser.add_argument("--dest-port", action="store", default=9200, help="Destination port [default: %(default)s]")
    parser.add_argument("--dest-index", action="store", default="", type=unicode, help="Destination index")
    parser.add_argument("--dest-batch-size", action="store", type=int, default=5000, help="Destination batchsize [default: %(default)s]")
    parser.add_argument("--dest-alias", action="store", help="Destination index alias (to be set after we have finished populating)")
    parser.add_argument("--dest-concurrency", action="store", type=int, default=4, help="Destination batchsize [default: %(default)s]")
    parser.add_argument("--dest-delete-index", action="store_true", help="Delete destination index at before starting")

    parser.add_argument("--query", action="store", type=unicode, default="", help="Query to use [if None is specified, a match_all will be used]")

    args = parser.parse_args()

    if args.src_index is None or len(args.src_index) == 0:
        raise Exception("--src-index must be specified!")

    if args.dest_index is None or len(args.dest_index) == 0:
        raise Exception("--dest-index must be specified!")

    dt_start = datetime.now()
    # copy mapping
    src_es_instance = get_elasticsearch(args.src_host, args.src_port)
    dest_es_instance = get_elasticsearch(args.dest_host, args.dest_port)
    # check if src_index exists
    src_es_ic = IndicesClient(src_es_instance)
    if not src_es_ic.exists(args.src_index):
        raise Exception("--src-index %s does not exist!" % args.src_index)
    # check if dest_index exists
    dest_es_ic = IndicesClient(dest_es_instance)
    if dest_es_ic.exists(args.dest_index):
        if args.dest_delete_index:
            dest_es_ic.delete(index=args.dest_index)
        else:
            raise Exception("--dest-index %s already exists! Use --dest-delete-index if you want to drop it" % args.dest_index)
    log.info("Copying mapping...")
    # copy mapping over to dest
    src_index_information = src_es_ic.get(index=args.src_index)
    dest_es_ic.create(index=args.dest_index, body=src_index_information.get(args.src_index, {}))
    # set num_of_replicas to 0
    dest_es_ic.put_settings(index=args.dest_index, body={"settings": {"index": {"number_of_replicas": 0}}})
    # perform multiprocessing
    log.info("Copying data...")
    MAGIC_STRING = "%s:%s" % (str(uuid4()), str(uuid4()))
    DEST_QUEUE = Queue()
    DEST_COUNTER = Value('i', 0)
    src_process = Process(target=src_worker, args=(args, DEST_QUEUE, MAGIC_STRING))
    src_process.start()
    dest_processes = [Process(target=dest_worker, args=(args, DEST_QUEUE, MAGIC_STRING, DEST_COUNTER)) for i in xrange(args.dest_concurrency)]
    for i in dest_processes: i.start()
    src_process.join()
    for i in dest_processes: i.join()
    log.info("[dest_worker] Total processed %s" % DEST_COUNTER.value)
    if args.dest_alias is not None and len(args.dest_alias) > 0:
        # we remove all existing mappings to this alias, then add it to the current dest_index
        for idx_name, aliases_mapping in dest_es_ic.get_aliases().iteritems():
            if args.dest_alias in aliases_mapping.get("aliases", {}):
                dest_es_ic.delete_alias(index=idx_name, name=args.dest_alias)
        dest_es_ic.put_alias(index=args.dest_index, name=args.dest_alias)
    dest_es_ic.refresh(args.dest_index)
    dt_end = datetime.now()
    log.info("Time elapsed: %s" % (dt_end-dt_start, ))

if __name__ == "__main__":
    main()