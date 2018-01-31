/usr/lib/spark/bin/spark-submit \
--master yarn-client \
--executor-memory 8g  --executor-cores 4  --num-executors 32 \
--jars spark-examples_2.10-2.0.0-SNAPSHOT.jar,random-0.0.1-SNAPSHOT-shaded.jar  \
--py-files python-lib.zip \
fix_feat_type.py  \
 $@
