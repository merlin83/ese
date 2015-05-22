ESE is an Elasticsearch exporter inspired by Elasticsearch-Exporter [https://github.com/mallocator/Elasticsearch-Exporter] and elasticsearch-dump [https://github.com/taskrabbit/elasticsearch-dump]

This script retrieves from the Elasticsearch scroll API to retrieve from the source and multiprocessing to index at the destination server. It uses RequestsHttpConnection for gzip compression.

# Installation

```
pip install ese
```

For the brave...

```
pip install -e git+https://github.com/merlin83/ese#egg=ese
```

# Usage
```
usage: ese.py [-h] [--src-host SRC_HOST] [--src-port SRC_PORT]
              [--src-index SRC_INDEX] [--src-batch-size SRC_BATCH_SIZE]
              [--dest-host DEST_HOST] [--dest-port DEST_PORT]
              [--dest-index DEST_INDEX] [--dest-batch-size DEST_BATCH_SIZE]
              [--dest-alias DEST_ALIAS] [--dest-concurrency DEST_CONCURRENCY]
              [--dest-delete-index] [--query QUERY]

optional arguments:
  -h, --help            show this help message and exit
  --src-host SRC_HOST   Source host [default: 127.0.0.1]
  --src-port SRC_PORT   Source port [default: 9200]
  --src-index SRC_INDEX
                        Source index
  --src-batch-size SRC_BATCH_SIZE
                        Source query batchsize [default: 5000]
  --dest-host DEST_HOST
                        Destination host [default: 127.0.0.1]
  --dest-port DEST_PORT
                        Destination port [default: 9200]
  --dest-index DEST_INDEX
                        Destination index
  --dest-batch-size DEST_BATCH_SIZE
                        Destination batchsize [default: 5000]
  --dest-alias DEST_ALIAS
                        Destination index alias (to be set after we have
                        finished populating)
  --dest-concurrency DEST_CONCURRENCY
                        Destination batchsize [default: 4]
  --dest-delete-index   Delete destination index at before starting
  --query QUERY         Query to use [if None is specified, a match_all will
                        be used]
```

# Example
```shell
ese \
    --src-host $SOURCE_HOST \
    --src-port $SOURCE_PORT \
    --src-index $SOURCE_INDEX \
    --src-batch-size 5000 \
    --dest-host $TARGET_HOST \
    --dest-port $TARGET_PORT \
    --dest-index $TARGET_INDEX \
    --dest-delete-index \
    --dest-concurrency 4 \
    --dest-batch-size 5000 \
    --dest-alias vts_companydirectory \
    --query $SOURCE_QUERY
```
