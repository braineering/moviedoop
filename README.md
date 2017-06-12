# MOVIEDOOP

*A map/reduce application for movie analytics, leveraging Hadoop and Movielens.*

*Coursework in Systems and Architectures for Big Data 2016/2017*


## Requirements
The system needs to be provided with the following packages:
* Java >= 1.8.0
* Maven >= 3.3.9
* Hadoop = 2.8.0
* Hive >= 2.1.1
* HBase >= 1.2.6
* Flume >= 1.7.0
* Postgresql >= 9.4.0

and the following environment variables, pointing to the respective package home directory:
* JAVA_HOME
* MAVEN_HOME
* HADOOP_HOME
* HIVE_HOME
* HBASE_HOME
* FLUME_HOME


## Build
Build the map/reduce driver for all queries:

    $> mvn clean package -P driver


## Usage
Start the environment:

    $moviedoop_home> bash start-env.sh

If it is the first environment setup, you need to run:

    $moviedoop_home> bash start-env.sh format

WARNING: notice that the last command will format your HDFS and Hive metastore.

The general job submission is as follows:

    $hadoop_home> bin/hadoop jar <MOVIEDOOP-JAR> <PROGRAM> [HADOOP_OPTS] [PROGRAM_OPTS] <ARGS>

where
* **[MOVIEDOOP-JAR]** is the local absolute path to the Mooviedoop's JAR;
* **[PROGRAM]** is the name of the map/reduce program to execute;
* **[HADOOP_OPTS]** are optional Hadoop options (e.g. -Dopt=val);
* **[PROGRAM_OPTS]** are optional program options (e.g. -D opt=val);
* **[ARGS]** are the mandatory program arguments.

Notice that the following map/reduce programs are available:
* **query1_1** the 1st query, leveraging inner join (repartition join).
* **query1_2** the 1st query, leveraging inner join (replication join, distributed cache on map).
* **query1_3** the 1st query, leveraging inner join (replication join, distributed cache on reduce).
* **query1_4** the 1st query, leveraging inner join (replication join, distributed cache on reduce) and optimizations on average computation (type 1).
* **query1_5** the 1st query, leveraging inner join (replication join, distributed cache on reduce) and optimizations on average computation (type 2).
* **query1_6** the 1st query, leveraging inner join (replication join, distributed cache on mapper), optimizations on average computation (type 2) and ORC serialization.
* **query2_1** the 2nd query, leveraging inner join (replication join, distributed cache on reduce)
* **query2_2** the 2nd query, leveraging inner join (replication join, distributed cache on reduce) and aggregation on ratings' score (type 1).
* **query2_3** the 2nd query, leveraging inner join (replication join, distributed cache on reduce) and aggregation type 1 and aggregation on ratings' movieID (type 2)
* **query2_4** the 2nd query, leveraging inner join (replication join, distributed cache on reduce) and aggregations type 1, type 2 and aggregations of genres (type 3).
* **query2_5** the 2nd query, leveraging inner join (replication join, distributed cache on reduce) and aggregations: type 1, type 2 and (type 3); and ORC serialization.
* **query3_1** the 3rd query, leveraging inner join (replication join, distributed cache on map) and BestMap for top-k.
* **query3_2** the 3rd query, leveraging inner join (replication join, distributed cache on map), BestMap for top-k and optimizations on average computation (type 1).
* **query3_3** the 3rd query, leveraging inner join (replication join, distributed cache on map), BestMap for top-k and optimizations on average computation (type 2).
* **query3_4** the 3rd query, leveraging inner join (replication join, distributed cache on map), BestMap for top-k, optimizations on average computation (type 2) and ORC serialization.
* **query3_5** the 3rd query, leveraging inner join (replication join, distributed cache on map), BestMap for top-k, optimizations on average computation (type 2), ORC serialization and parallel jobs.

Read the output:

    $hadoop_home> bin/hadoop hdfs -cat [RESULT]/*

where
*[RESULT]* is the HDFS directory of results.

Stop the environment:

    $moviedoop_home> bash stop-env.sh


### Query1

    $hadoop_home> bin/hadoop jar <MOVIEDOOP-JAR> query1_6 [HADOOP_OPTS] [PROGRAM_OPTS] <IN_RATINGS> <IN_MOVIES> <OUT>

where:
* **[MOVIEDOOP-JAR]** is the local absolute path to the Mooviedoop's JAR;
* **[HADOOP_OPTS]** are optional Hadoop options (e.g. -Dopt=val);
* **[PROGRAM_OPTS]** are optional program options (e.g. -D opt=val);
* **[IN\_RATINGS]** is the HDFS absolute path to the directory containing the ratings data set;
* **[IN\_MOVIES]** is the HDFS absolute path to the directory containing the movies data set;
* **[OUT]** is the HDFS absolute path to the directory for the output.

Important note: query1_1 accepts only Text input files, while query1_6 accepts only ORC input files.

The following program options are available:
* `moviedoop.average.rating.lb`: the lower bound for the movie average rating;
* `moviedoop.average.rating.timestamp.lb`: the lower bound for the movie rating timestamp (e.g. dd/mm/yyyy or dd/mm/yyyyThh:mm:ss);
* `moviedoop.average.reduce.cardinality`: the number of reducers for the average job.

Here is an example:

    $hadoop_home> bin/hadoop jar moviedoop-1.0.jar \
    query1_6 \
    -D moviedoop.average.rating.lb=2.5 \
    -D moviedoop.average.rating.timestamp.lb=01/01/1970 \
    -D moviedoop.average.reduce.cardinality=2 \
    /hdfs/path/to/ratings \
    /hdfs/path/to/movies \
    /hdfs/path/to/query1


### Query2

    $hadoop_home> bin/hadoop jar <MOVIEDOOP-JAR> query2_5 [HADOOP_OPTS] [PROGRAM_OPTS] <IN_RATINGS> <IN_MOVIES> <OUT>

where:
* **[MOVIEDOOP-JAR]** is the local absolute path to the Mooviedoop's JAR;
* **[HADOOP_OPTS]** are optional Hadoop options (e.g. -Dopt=val);
* **[PROGRAM_OPTS]** are optional program options (e.g. -D opt=val);
* **[IN\_RATINGS]** is the HDFS absolute path to the directory containing the ratings data set;
* **[IN\_MOVIES]** is the HDFS absolute path to the directory containing the movies data set;
* **[OUT]** is the HDFS absolute path to the directory for the output.

Important note: query2_1 accepts only Text input files, while query2_5 accepts only ORC input files.

The following program options are available:
* `moviedoop.ratings.reduce.cardinality`: the number of reducers for the ratings job.
* `moviedoop.average.reduce.cardinality`: the number of reducers for the average job.

Here is an example:

    $hadoop_home> bin/hadoop jar moviedoop-1.0.jar \
    query2_5 \
    -D moviedoop.ratings.reduce.cardinality=2 \
    -D moviedoop.average.reduce.cardinality=2 \
    /hdfs/path/to/ratings \
    /hdfs/path/to/movies \
    /hdfs/path/to/query2


### Query3

    $hadoop_home> bin/hadoop jar <MOVIEDOOP-JAR> query3_5 [HADOOP_OPTS] [PROGRAM_OPTS] <IN_RATINGS> <IN_MOVIES> <OUT>

where:
* **[MOVIEDOOP-JAR]** is the local absolute path to the Mooviedoop's JAR;
* **[HADOOP_OPTS]** are optional Hadoop options (e.g. -Dopt=val);
* **[PROGRAM_OPTS]** are optional program options (e.g. -D opt=val);
* **[IN\_RATINGS]** is the HDFS absolute path to the directory containing the ratings data set;
* **[IN\_MOVIES]** is the HDFS absolute path to the directory containing the movies data set;
* **[OUT]** is the HDFS absolute path to the directory for the output.

Important note: query3_1 accepts only Text input files, while query3_5 accepts only ORC input files.

The following program options are available:
* `moviedoop.topk.size`: the movies top rank size;
* `moviedoop.average.rating.timestamp.lb.1`: the lower bound for the movie rating timestamp considered for top ranking (e.g. dd/mm/yyyy or dd/mm/yyyyThh:mm:ss);
* `moviedoop.average.rating.timestamp.ub.1`: the upper bound for the movie rating timestamp considered for top ranking (e.g. dd/mm/yyyy or dd/mm/yyyyThh:mm:ss);
* `moviedoop.average.rating.timestamp.lb.2`: the lower bound for the movie rating timestamp considered for total ranking (e.g. dd/mm/yyyy or dd/mm/yyyyThh:mm:ss);
* `moviedoop.average.rating.timestamp.ub.2`: the upper bound for the movie rating timestamp considered for total ranking (e.g. dd/mm/yyyy or dd/mm/yyyyThh:mm:ss);
* `moviedoop.average.reduce.cardinality`: the number of reducers for the average job;
* `moviedoop.topk.reduce.cardinality`: the number of reducers for the topk job;
* `moviedoop.sort.reduce.cardinality`: the number of reducers for the sort job;
* `moviedoop.sort.partition.samples`: the number of samples for the total sort partition;
* `moviedoop.sort.partition.frequency`: the frequency for the total sort partition;
* `moviedoop.sort.partition.splits.max`: the maximum number of splits for the total sort partition.

Here is an example:

    $hadoop_home> bin/hadoop jar moviedoop-1.0.jar \
    query3_5 \
    -D moviedoop.topk.size=10 \
    -D moviedoop.average.rating.timestamp.lb.1=01/01/1990 \
    -D moviedoop.average.rating.timestamp.ub.1=01/01/1991 \
    -D moviedoop.average.rating.timestamp.lb.2=01/01/1988 \
    -D moviedoop.average.rating.timestamp.ub.2=01/01/1989 \
    -D moviedoop.average.reduce.cardinality=2 \
    -D moviedoop.topk.reduce.cardinality=2 \
    -D moviedoop.sort.reduce.cardinality=2 \
    -D moviedoop.sort.partition.samples=1000 \
    -D moviedoop.sort.partition.frequency=0.01 \
    -D moviedoop.sort.partition.splits.max=100 \
    /hdfs/path/to/ratings \
    /hdfs/path/to/movies \
    /hdfs/path/to/query3


### Data Ingestion/Exportation

The environment setup activates both the data ingestion and exportation.
In particular it activates the following Flume agents:
* **movies_agent**: imports movies dataset from the local spooldir `/path/to/moviedoop/data/flume/movies` to the HDFS directory `/user/flume/movies` as an external Hive table in sequence file format;
* **movies_agent**: imports ratings dataset from the local spooldir `/path/to/moviedoop/data/flume/ratings` to the HDFS directory `/user/flume/ratings` as an external Hive table in  sequence file format;
* **query1_agent**: exports the results of query1 from `hdfs:///user/moviedoop/output/query1` to the HBase table `query1`;
* **query2_agent**: exports the results of query2 from `hdfs:///user/moviedoop/output/query2` to the HBase table `query2`;
* **query3_agent**: exports the results of query3 from `hdfs:///user/moviedoop/output/query3` to the HBase table `query3`.


## Evaluation
The performance of all queries can be evaluated running the bash scripts in folder `eval/`.
Every evaluation script compares the baseline and the optimized implementation of a specific query,
generating a report file in the same directory.
For user's convenience, all the evaluation reports have been included in `eval/out`
The following evaluation scripts are available:
* **eval_q1.sh**: evaluates query1 by comparing the performance of the baseline implementation (1.1) with the best implementation (1.6);
* **eval_q2.sh**: evaluates query1 by comparing the performance of the baseline implementation (2.1) with the best implementation (2.5);
* **eval_q3.sh**: evaluates query1 by comparing the performance of the baseline implementation (3.1) with the best implementation (3.5);


## Authors
Giacomo Marciani, [gmarciani@acm.org](mailto:gmarciani@acm.org)

Michele Porretta, [mporretta@acm.org](mailto:mporretta@acm.org)


## References
Systems and Architectures for Big Data, course by prof. Valeria Cardellini. 2016/2017 [Read here](http://www.ce.uniroma2.it/courses/sabd1617/)


## License
The project is released under the [MIT License](https://opensource.org/licenses/MIT).
