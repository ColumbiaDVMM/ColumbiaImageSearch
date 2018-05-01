/usr/lib/spark/bin/spark-submit \
 --master yarn-client \
--executor-memory 10g  --executor-cores 2  --num-executors 190 \
--jars spark-examples-1.6.0-cdh5.10.0-hadoop2.6.0-cdh5.10.0.jar,random-0.0.1-SNAPSHOT-shaded.jar   \
--py-files python-lib.zip \
transform-ht-images-table-to-transition_forawscloud.py  \
 $@