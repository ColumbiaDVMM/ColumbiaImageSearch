/usr/lib/spark/bin/spark-submit \
 --master yarn-client \
--executor-memory 20g  --executor-cores 4  --num-executors 120 \
--jars spark-examples-1.6.0-cdh5.10.0-hadoop2.6.0-cdh5.10.0.jar,random-0.0.1-SNAPSHOT-shaded.jar   \
--py-files python-lib.zip \
count_extractions.py  \
 $@