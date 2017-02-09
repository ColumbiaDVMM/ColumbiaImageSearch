/usr/lib/spark/bin/spark-submit \
 --master yarn-client \
--executor-memory 20g  --executor-cores 5  --num-executors 4 \
--jars spark-examples_2.10-2.0.0-SNAPSHOT.jar,random-0.0.1-SNAPSHOT-shaded.jar  \
--py-files python-lib.zip \
fill_sha1_from_s3url.py  \
 $@
