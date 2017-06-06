#!/bin/bash

##
# EVAL_Q1
##

MOVIEDOOP_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
MOVIEDOOP_JAR="${MOVIEDOOP_HOME}/target/moviedoop-1.0.jar"

HADOOP_JAR="${HADOOP_HOME}/bin/hadoop jar"
HDFS_WAREHOUSE="/user/hive/warehouse"
HDFS_OUT="/moviedoop/output/eval_q1"

HDFS_RM="${HADOOP_HOME}/bin/hdfs dfs -rm -r -f"

OUT_EVALUATION="eval_q1.out"
rm ${OUT_EVALUATION}

QUERIES=( "query1_1" "query1_6" )
SLEEP_BETWEEN_QUERIES=20s

for QUERY in "${QUERIES[@]}"; do
    OPT_AVERAGE_REDUCE_CARDINALITY="-Dmoviedoop.average.reduce.cardinality"
    AVERAGE_REDUCE_CARDINALITIES=( "1" "2" "4" "8" "16" )

    if [ ${QUERY} = "query1_1" ]; then
        IN_RATINGS="${HDFS_WAREHOUSE}/ratings_test"
        IN_MOVIES="${HDFS_WAREHOUSE}/movies_test"
    elif [ ${QUERY} = "query1_6" ]; then
        IN_RATINGS="${HDFS_WAREHOUSE}/ratings_test_orc"
        IN_MOVIES="${HDFS_WAREHOUSE}/movies_test_orc"
    fi

    for AVERAGE_REDUCE_CARDINALITY in "${AVERAGE_REDUCE_CARDINALITIES[@]}"; do
        OUT_QUERY="${HDFS_OUT}/${QUERY}.${AVERAGE_REDUCE_CARDINALITY}"
        $HDFS_RM "${OUT_QUERY}*"
        OPTS=""
        OPTS="${OPTS} ${OPT_AVERAGE_REDUCE_CARDINALITY}=${AVERAGE_REDUCE_CARDINALITY}"
        START="$( date +%s )"
        ${HADOOP_JAR} ${MOVIEDOOP_JAR} ${QUERY} ${OPTS} ${IN_RATINGS} ${IN_MOVIES} ${OUT_QUERY}
        END="$( date +%s )"
        ELAPSED="$(( ${END} - ${START} ))"
        echo "${QUERY} (${OPTS}) : ${ELAPSED} seconds" >> ${OUT_EVALUATION}
        sleep ${SLEEP_BETWEEN_QUERIES}
    done
done
