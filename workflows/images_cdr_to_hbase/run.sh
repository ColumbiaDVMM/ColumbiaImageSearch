/usr/lib/spark/bin/spark-submit \
 --master yarn-client \
--executor-memory 20g  --executor-cores 5  --num-executors 80 \
--jars elasticsearch-hadoop-2.2.0-m1.jar,spark-examples_2.10-2.0.0-SNAPSHOT.jar,random-0.0.1-SNAPSHOT-shaded.jar  \
--py-files ../python-lib.zip \
get_cdr2_list_indices_images_to_hbase.py  \
$@
