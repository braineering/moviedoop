# MOVIEDOOP

*A batch processing application for movie analytics, leveraging Hadoop.*

*Coursework in Systems and Architectures for Big Data 2016/2017*

Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.


## Installation
The system needs to be provided with the following:
* Java >= 1.8.0
* Maven >= 3.3.9
* Hadoop 3.0.0.0-alpha2


## Build
Build the job:

    $> mvn clean package -P [JOB]
    
where *[JOB]* is the name of the job to build.
The following jobs are available:
* **query1_1** the 1st query with no inner join.
* **query1_2** the 1st query with inner join (repartition join).
* **query1_3** the 1st query with inner join (replication join, distributed cache on reducer).
* **query1_4** the 1st query with inner join (replication join, distributed cache on mapper).
* **query2** the 2nd query.
* **query3** the 3rd query with no inner join.
* **query3_bis** the 3rd query with inner join.


## Usage
Start Hadoop:
    
    $hadoop_home> sbin/start-dfs.sh

Submit the job:

    $hadoop_home> bin/hadoop jar [JOB-JAR] [INDIR] [OUTDIR] [ARGS]
    
where 
*[JOB-JAR]* is the local absolute path to the job's JAR, 
*[INDIR]* is the HDFS folder cotaining the input files,
*[OUTDIR]* is the HDFS destination folder for the output and
*[ARGS]* are other command line arguments.

Read the output:

    $hadoop_home> bin/hadoop hdfs -cat [RESULT]
    
where
*[RESULT]* is the HDFS file of results (e.g. *[OUTDIR]/part-r-00000*).

Stop Hadoop:

    $hadoop_home> sbin/stop-dfs.sh


### Query1

    $hadoop_home> bin/hadoop jar [JAR] [RATINGS] [OUTDIR] [SCORE] (START_DATE)
    
where 
*[JAR]* is the local absolute path to the JAR,
*[RATINGS]* is the HDFS absolute path to the directory containing the ratings data set,
*[OUTDIR]* is the HDFS absolute path to the directory for the output,
*[SCORE]* is the rating threshold and 
*(START\_DATE)* is the optional lower bound for the timestamp (i.e. dd/mm/yyyy or dd/mm/yyyyThh:mm:ss).


### Query1_2

    $hadoop_home> bin/hadoop jar [JAR] [RATINGS] [MOVIES] [OUTDIR] [SCORE] (START_DATE)
    
where 
*[JAR]* is the local absolute path to the JAR,
*[RATINGS]* is the HDFS absolute path to the directory containing the ratings data set,
*[RATINGS]* is the HDFS absolute path to the directory containing the movies data set,
*[OUTDIR]* is the HDFS absolute path to the directory for the output,
*[SCORE]* is the rating threshold and 
*(START\_DATE)* is the optional lower bound for the timestamp (i.e. dd/mm/yyyy or dd/mm/yyyyThh:mm:ss).


## Authors
Giacomo Marciani, [gmarciani@acm.org](mailto:gmarciani@acm.org)

Michele Porretta, [mporretta@acm.org](mailto:mporretta@acm.org)


## References
Giacomo Marciani, Marco Piu, Michele Porretta, Matteo Nardelli, and Valeria Cardellini. 2016. Real-time analysis of social networks leveraging the flink framework. In *Proceedings of the 10th ACM International Conference on Distributed and Event-based Systems (DEBS '16)*. ACM, New York, NY, USA, 386-389. [DOI](http://dx.doi.org/10.1145/2933267.2933517) [Read here](http://dl.acm.org/citation.cfm?id=2933517)


## License
The project is released under the [MIT License](https://opensource.org/licenses/MIT).
