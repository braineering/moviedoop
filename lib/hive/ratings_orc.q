DROP TABLE ratings_orc;

CREATE TABLE ratings_orc(userId STRING, movieId STRING, rating STRING, time STRING)
STORED AS ORC;

INSERT INTO TABLE ratings_orc SELECT * FROM ratings;
