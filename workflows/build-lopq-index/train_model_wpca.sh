/usr/lib/spark/bin/spark-submit \
--master yarn-client \
--executor-memory 10g  --executor-cores 4 --num-executors 240 --driver-memory 20g \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=hdfs://memex/user/spark/applicationHistory \
--conf spark.yarn.historyServer.address=memex-spark-master.xdata.data-tactics-corp.com:18080 \
--conf spark.logConf=true \
--jars spark-examples-1.6.0-cdh5.10.0-hadoop2.6.0-cdh5.10.0.jar,random-0.0.1-SNAPSHOT-shaded.jar \
--py-files lopq.zip \
train_model_wpca.py $@
