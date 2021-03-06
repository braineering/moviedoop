#!/bin/bash

MOVIEDOOP_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

MOVIEDOOP_JAR="${MOVIEDOOP_HOME}/target/moviedoop-1.0.jar"

##
# HADOOP COMMANDS
##
HADOOP_JAR="${HADOOP_HOME}/bin/hadoop jar"
HDFS_RM="${HADOOP_HOME}/bin/hdfs dfs -rm -r -f"

##
# DATASET
##
${HIVE_HOME}/bin/hive -f ${MOVIEDOOP_HOME}/lib/hive/movies_orc.q
${HIVE_HOME}/bin/hive -f ${MOVIEDOOP_HOME}/lib/hive/ratings_orc.q

##
# INPUT
##
HDFS_WAREHOUSE="/user/hive/warehouse"
IN_RATINGS="${HDFS_WAREHOUSE}/ratings_orc"
IN_MOVIES="${HDFS_WAREHOUSE}/movies_orc"

##
# OUTPUT
##
HDFS_OUT="/user/moviedoop/output"

##
# QUERY 1
##
QUERY="query1_6"
OPT_AVERAGE_REDUCE_CARDINALITY="-Dmoviedoop.average.reduce.cardinality"
AVERAGE_REDUCE_CARDINALITY=2
OPTS="${OPT_AVERAGE_REDUCE_CARDINALITY}=${AVERAGE_REDUCE_CARDINALITY}"
QUERY_OUT="${HDFS_OUT}/${QUERY}"
$HDFS_RM "${QUERY_OUT}*"
${HADOOP_JAR} ${MOVIEDOOP_JAR} ${QUERY} ${OPTS} ${IN_RATINGS} ${IN_MOVIES} ${QUERY_OUT}

##
# QUERY 2
##
QUERY="query2_5"
OPT_RATINGS_REDUCE_CARDINALITY="-Dmoviedoop.ratings.reduce.cardinality"
RATINGS_REDUCE_CARDINALITY=2
OPT_AVERAGE_REDUCE_CARDINALITY="-Dmoviedoop.average.reduce.cardinality"
AVERAGE_REDUCE_CARDINALITY=2
OPTS="${OPT_RATINGS_REDUCE_CARDINALITY}=${RATINGS_REDUCE_CARDINALITY}"
OPTS="${OPTS} ${OPT_AVERAGE_REDUCE_CARDINALITY}=${AVERAGE_REDUCE_CARDINALITY}"
QUERY_OUT="${HDFS_OUT}/${QUERY}"
$HDFS_RM "${QUERY_OUT}*"
${HADOOP_JAR} ${MOVIEDOOP_JAR} ${QUERY} ${OPTS} ${IN_RATINGS} ${IN_MOVIES} ${QUERY_OUT}

##
# QUERY 3
##
QUERY="query3_5"
OPT_AVERAGE_REDUCE_CARDINALITY="-Dmoviedoop.average.reduce.cardinality"
AVERAGE_REDUCE_CARDINALITY=2
OPT_TOPK_REDUCE_CARDINALITY="-Dmoviedoop.topk.reduce.cardinality"
TOPK_REDUCE_CARDINALITY=2
OPT_SORT_REDUCE_CARDINALITY="-Dmoviedoop.sort.reduce.cardinality"
SORT_REDUCE_CARDINALITY=2
OPTS="${OPT_AVERAGE_REDUCE_CARDINALITY}=${AVERAGE_REDUCE_CARDINALITY}"
OPTS="${OPTS} ${OPT_TOPK_REDUCE_CARDINALITY}=${TOPK_REDUCE_CARDINALITY}"
OPTS="${OPTS} ${OPT_SORT_REDUCE_CARDINALITY}=${SORT_REDUCE_CARDINALITY}"
QUERY_OUT="${HDFS_OUT}/${QUERY}"
$HDFS_RM "${QUERY_OUT}*"
${HADOOP_JAR} ${MOVIEDOOP_JAR} ${QUERY} ${OPTS} ${IN_RATINGS} ${IN_MOVIES} ${QUERY_OUT}
