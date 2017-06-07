DROP TABLE ratings;
DROP TABLE ratings_orc;

CREATE TABLE ratings(userId STRING, movieId STRING, rating STRING, time STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/gmarciani/Workspace/moviedoop/_ignore/data/ratings.csv' INTO TABLE ratings;

CREATE TABLE ratings_orc(userId STRING, movieId STRING, rating STRING, time STRING)
STORED AS ORC;

INSERT INTO TABLE ratings_orc SELECT * FROM ratings;
