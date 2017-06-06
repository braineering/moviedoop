#!/bin/bash

MOVIEDOOP_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$HADOOP_HOME/sbin/stop-dfs.sh
$HADOOP_HOME/sbin/stop-yarn.sh
sudo rm -rf /tmp/*

$HADOOP_HOME/bin/hdfs namenode -format -force
$HADOOP_HOME/sbin/start-dfs.sh

$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop
$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop/dist
$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop/output

$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop/test
$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop/test/output

$HADOOP_HOME/bin/hdfs dfs -mkdir /user/
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/hive
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/hive/warehouse
$HADOOP_HOME/bin/hdfs dfs -mkdir /tmp
$HADOOP_HOME/bin/hdfs dfs -chmod g+w /user/hive/warehouse
$HADOOP_HOME/bin/hdfs dfs -chmod g+w /tmp

rm -rf $HIVE_HOME/metastore_db
rm     $HIVE_HOME/derby.log
rm -rf metastore_db
rm     derby.log
$HIVE_HOME/bin/schematool -initSchema -dbType derby

$HIVE_HOME/bin/hive -f $MOVIEDOOP_HOME/data/hive/movies_test.q
$HIVE_HOME/bin/hive -f $MOVIEDOOP_HOME/data/hive/ratings_test.q
$HIVE_HOME/bin/hive -f $MOVIEDOOP_HOME/data/hive/movies.q
$HIVE_HOME/bin/hive -f $MOVIEDOOP_HOME/data/hive/ratings.q

$HBASE_HOME/bin/start-hbase.sh