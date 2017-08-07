/usr/lib/spark/bin/spark-submit \
--master yarn-client \
--executor-memory 40g  --executor-cores 2 --num-executors 40 --driver-memory 40g \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=hdfs://memex/user/spark/applicationHistory \
--conf spark.yarn.historyServer.address=memex-spark-master.xdata.data-tactics-corp.com:18080 \
--conf spark.logConf=true \
--conf spark.driver.maxResultSize=2g \
--jars hdfs://memex/user/skaraman/packages/spark-examples-1.6.0-cdh5.10.0-hadoop2.6.0-cdh5.10.0.jar,hdfs://memex/user/skaraman/packages/random-0.0.1-SNAPSHOT-shaded.jar \
--py-files cufacesearch.zip,update_wurl.pkl \
faces_detect_featurize.py \
$@
