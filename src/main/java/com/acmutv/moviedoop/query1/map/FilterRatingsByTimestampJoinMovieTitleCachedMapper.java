/*
  The MIT License (MIT)

  Copyright (c) 2017 Giacomo Marciani and Michele Porretta

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:


  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.


  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
 */
package com.acmutv.moviedoop.query1.map;

import com.acmutv.moviedoop.query1.Query1_3;
import com.acmutv.moviedoop.common.util.DateParser;
import com.acmutv.moviedoop.common.util.RecordParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * The mapper for jobs in: {@link Query1_3}.
 * It emits (movieTitle,rating) where movieTitle is joined with the movies cached files and rating
 * is a score attributed with timestamp greater or equal to the `movieRatingTimestampLowerBound`.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class FilterRatingsByTimestampJoinMovieTitleCachedMapper extends Mapper<Object,Text,Text,DoubleWritable> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(FilterRatingsByTimestampJoinMovieTitleCachedMapper.class);

  /**
   * The cached map (movieId,movieTitle).
   */
  private Map<Long,String> movieIdToMovieTitle = new HashMap<>();

  /**
   * The lower bound for the movie rating timestamp.
   */
  private long movieRatingTimestampLowerBound;

  /**
   * The movie title to emit.
   */
  private Text movieTitle = new Text();

  /**
   * The movie rating to emit.
   */
  private DoubleWritable movieRating = new DoubleWritable();

  /**
   * Configures the mapper.
   * @param ctx the job context.
   */
  protected void setup(Context ctx) {
    this.movieRatingTimestampLowerBound =
        DateParser.toSeconds(ctx.getConfiguration().get("moviedoop.average.rating.timestamp.lb"));
    LOG.debug("[SETUP] moviedoop.average.rating.timestamp.lb: " + this.movieRatingTimestampLowerBound);

    try {
      for (URI uri : ctx.getCacheFiles()) {
        Path path = new Path(uri);
        BufferedReader br = new BufferedReader(
            new InputStreamReader(
                new FileInputStream(path.getName())));
        String line;
        while ((line = br.readLine()) != null) {
          Map<String,String> movie = RecordParser.parse(line, new String[] {"id","title","genres"},RecordParser.ESCAPED_DELIMITER);
          long movieId = Long.valueOf(movie.get("id"));
          String movieTitle = movie.get("title");
          this.movieIdToMovieTitle.put(movieId, movieTitle);
        }
        br.close();
      }
    } catch (IOException exc) {
      exc.printStackTrace();
    }
  }

  /**
   * The mapping routine.
   *
   * @param key the input key.
   * @param value the input value.
   * @param ctx the context.
   * @throws IOException when the context cannot be written.
   * @throws InterruptedException when the context cannot be written.
   */
  public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
    Map<String,String> rating = RecordParser.parse(value.toString(), new String[] {"userId","movieId","score","timestamp"}, ",");

    long timestamp = Long.valueOf(rating.get("timestamp"));
    if (timestamp >= this.movieRatingTimestampLowerBound) {
      long movieId = Long.valueOf(rating.get("movieId"));
      double score = Double.valueOf(rating.get("score"));
      this.movieTitle.set(this.movieIdToMovieTitle.getOrDefault(movieId, "N/A-"+movieId));
      this.movieRating.set(score);
      ctx.write(this.movieTitle, this.movieRating);
    }
  }
}
