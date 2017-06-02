DROP TABLE movies;
DROP TABLE movies_orc;

CREATE TABLE movies(id STRING, title STRING, genres STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/gmarciani/Workspace/moviedoop/_ignore/data/movies.csv' INTO TABLE movies;

CREATE TABLE movies_orc(id STRING, title STRING, genres STRING)
STORED AS ORC;

INSERT INTO TABLE movies_orc SELECT * FROM movies;