DROP TABLE movies_orc;

CREATE TABLE movies_orc(id STRING, title STRING, genres STRING)
STORED AS ORC;

INSERT INTO TABLE movies_orc SELECT * FROM movies;
