#!/bin/bash

$HADOOP_HOME/sbin/stop-dfs.sh
$HADOOP_HOME/sbin/stop-yarn.sh
sudo rm -rf /tmp/*

$HADOOP_HOME/bin/hdfs namenode -format -force
$HADOOP_HOME/sbin/start-dfs.sh

#$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave

$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop
$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop/input
$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop/input/movies
$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop/input/ratings
$HADOOP_HOME/bin/hdfs dfs -put /home/gmarciani/Workspace/moviedoop/_ignore/data/movies.csv /moviedoop/input/movies
$HADOOP_HOME/bin/hdfs dfs -put /home/gmarciani/Workspace/moviedoop/_ignore/data/ratings.csv /moviedoop/input/ratings
$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop/output

$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop/test
$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop/test/input
$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop/test/input/movies
$HADOOP_HOME/bin/hdfs dfs -mkdir /moviedoop/test/input/ratings
$HADOOP_HOME/bin/hdfs dfs -put /home/gmarciani/Workspace/moviedoop/data/test/movies.csv /moviedoop/test/input/movies
$HADOOP_HOME/bin/hdfs dfs -put /home/gmarciani/Workspace/moviedoop/data/test/ratings.csv /moviedoop/test/input/ratings
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
$HIVE_HOME/bin/hive -f /home/gmarciani/Workspace/moviedoop/data/hive/movies.q
$HIVE_HOME/bin/hive -f /home/gmarciani/Workspace/moviedoop/data/hive/ratings.q

