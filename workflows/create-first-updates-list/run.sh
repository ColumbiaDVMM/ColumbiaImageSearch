/usr/lib/spark/bin/spark-submit \
 --master yarn-client \
--jars spark-examples_2.10-2.0.0-SNAPSHOT.jar,random-0.0.1-SNAPSHOT-shaded.jar  \
--py-files python-lib.zip \
create-first-updates-list.py  \
 $@
