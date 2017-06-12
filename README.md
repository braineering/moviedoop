# MOVIEDOOP

*A batch processing application for movie analytics, leveraging Hadoop.*

*Coursework in Systems and Architectures for Big Data 2016/2017*


## Requirements
The system needs to be provided with the following packages:
* Java >= 1.8.0
* Maven >= 3.3.9
* Hadoop = 2.8.0
* Hive >= 2.1.1
* HBase >= 1.2.6
* Flume >= 1.7.0
* Postgres >= 9.4.0

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
    
If you want to reformat HDFS and setup the Hive metastore for the first time, you need to run: 

    $moviedoop_home> bash start-env.sh format
    
WARNING: notice that this last command will format your HDFS.



Submit the job:

    $hadoop_home> bin/hadoop jar <MOVIEDOOP-JAR> <PROGRAM> [HADOOP_OPTS] [PROGRAM_OPTS] <ARGS>
    
where 
* *[MOVIEDOOP-JAR]* is the local absolute path to the Mooviedoop's JAR, 
* *[PROGRAM]* is the name of the map/reduce program to execute,
* *[HADOOP_OPTS]* are optional Hadoop options specified as `-Dopt=val`,
* *[PROGRAM_OPTS]* are optional program options specified as `-D opt=val`,
* *[ARGS]* are the mandatory program arguments.

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
    
    
### Moviedoop as a single job
Moviedoop's queries can be executed



### Query1

    $hadoop_home> bin/hadoop jar <MOVIEDOOP-JAR> query1_1 [HADOOP_OPTS] [PROGRAM_OPTS] <IN_RATINGS> <IN_MOVIES> <OUT>
    
where:
* *[MOVIEDOOP-JAR]* is the local absolute path to the Mooviedoop's JAR,
* *[HADOOP_OPTS]* are optional Hadoop options specified as `-Dopt=val`,
* *[PROGRAM_OPTS]* are optional program options specified as `-D opt=val`,
* *[IN\_RATINGS]* is the HDFS absolute path to the directory containing the ratings data set,
* *[IN\_MOVIES]* is the HDFS absolute path to the directory containing the movies data set,
* *[OUT]* is the HDFS absolute path to the directory for the output.

Notice that the following program options are available:
* `movie.rating.average.lb`: the lower bound for the movie average rating;
* `movie.rating.timestamp.lb`: the lower bound for the movie rating timestamp (e.g. dd/mm/yyyy or dd/mm/yyyyThh:mm:ss).

Here is an example:

    $hadoop_home> bin/hadoop jar moviedoop-1.0.jar \
    query1_1 \
    -D movie.rating.average.lb=2.5 \
    -D movie.rating.timestamp.lb=01/01/1970 \
    /moviedoop/_test/input/ratings \
    /moviedoop/_test/input/movies \
    /moviedoop/_test/output/query1_1


### Query2

    $hadoop_home> bin/hadoop jar <MOVIEDOOP-JAR> query2_1 <IN_RATINGS> <IN_MOVIES> <OUT>
    
where:
* *[MOVIEDOOP-JAR]* is the local absolute path to the Mooviedoop's JAR,
* *[IN\_RATINGS]* is the HDFS absolute path to the directory containing the ratings data set,
* *[IN\_MOVIES]* is the HDFS absolute path to the directory containing the movies data set,
* *[OUT]* is the HDFS absolute path to the directory for the output.

Here is an example:

    $hadoop_home> bin/hadoop jar moviedoop-1.0.jar \
    query2_1 \
    /moviedoop/_test/input/ratings \
    /moviedoop/_test/input/movies \
    /moviedoop/_test/output/query2_1

### Query3

    $hadoop_home> bin/hadoop jar <MOVIEDOOP-JAR> query3_1 [HADOOP_OPTS] [PROGRAM_OPTS] <IN_RATINGS> <IN_MOVIES> <OUT>
    
where:
* *[MOVIEDOOP-JAR]* is the local absolute path to the Mooviedoop's JAR,
* *[HADOOP_OPTS]* are optional Hadoop options specified as `-Dopt=val`,
* *[PROGRAM_OPTS]* are optional program options specified as `-D opt=val`,
* *[IN\_RATINGS]* is the HDFS absolute path to the directory containing the ratings data set,
* *[IN\_MOVIES]* is the HDFS absolute path to the directory containing the movies data set,
* *[OUT]* is the HDFS absolute path to the directory for the output.

Notice that the following program options are available:
* `movie.topk.size`: the movies top rank size;
* `movie.topk.rating.timestamp.lb`: the lower bound for the movie rating timestamp considered for top ranking (e.g. dd/mm/yyyy or dd/mm/yyyyThh:mm:ss);
* `movie.topk.rating.timestamp.ub`: the upper bound for the movie rating timestamp considered for top ranking (e.g. dd/mm/yyyy or dd/mm/yyyyThh:mm:ss);
* `movie.rank.rating.timestamp.lb`: the lower bound for the movie rating timestamp considered for total ranking (e.g. dd/mm/yyyy or dd/mm/yyyyThh:mm:ss);
* `movie.rank.rating.timestamp.ub`: the upper bound for the movie rating timestamp considered for total ranking (e.g. dd/mm/yyyy or dd/mm/yyyyThh:mm:ss);

Here is an example:

    $hadoop_home> bin/hadoop jar moviedoop-1.0.jar \
    query3_1 \
    -D movie.topk.size=10 \
    -D movie.topk.rating.timestamp.lb=01/01/1990 \
    -D movie.topk.rating.timestamp.lb=01/01/1991 \
    -D movie.rank.rating.timestamp.lb=01/01/1988 \
    -D movie.rank.rating.timestamp.lb=01/01/1989 \
    /moviedoop/_test/input/ratings \
    /moviedoop/_test/input/movies \
    /moviedoop/_test/output/query3_1
    
### Data Injestion

Required:

*[Put data with Flume on HDFS from local file system:]* 

 USAGE:													
 1) Put configuration file (./flume/moviedoop.conf) in the $FLUME_HOME/conf directory 											
 2) Change the spoolDir path for setting the source directory
 3) Change path of HDFS sink
 4) Create a directory in HDFS with: $hdfs dfs -mkdir /namedirectory_of_sink						
 5) Run:
		$ cd $FLUME_HOME/bin (if you have FLUME_HOME as environment variable)
		$ ./flume-ng agent -n movieagent -f $FLUME_HOME/conf/moviedoop.conf -c $FLUME_HOME/conf

*[Put data with Flume on HBase from HDFS:]*
 
 USAGE:													
 1) Put configuration file (./flume/moviedoop.conf) in the $FLUME_HOME/conf directory 											
 2) Change path of HDFS source
 3) Create table on HBase with command: create 'movietable','d'
 4) Run:
        $ cd $FLUME_HOME/bin (if you have FLUME_HOME as environment variable)
        $ ./flume-ng agent -n movieagent2 -f $FLUME_HOME/conf/moviedoop.conf -c $FLUME_HOME/conf
        		
        		
## Evaluation
The performance of all queries can be evaluated leveraging the bash scripts in folder `eval/`. 
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
Giacomo Marciani, Marco Piu, Michele Porretta, Matteo Nardelli, and Valeria Cardellini. 2016. Real-time analysis of social networks leveraging the flink framework. In *Proceedings of the 10th ACM International Conference on Distributed and Event-based Systems (DEBS '16)*. ACM, New York, NY, USA, 386-389. [DOI](http://dx.doi.org/10.1145/2933267.2933517) [Read here](http://dl.acm.org/citation.cfm?id=2933517)


## License
The project is released under the [MIT License](https://opensource.org/licenses/MIT).
