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
package com.acmutv.moviedoop.map;

import com.acmutv.moviedoop.Query1_2;
import com.acmutv.moviedoop.model.RatingsWritable;
import com.acmutv.moviedoop.util.DateParser;
import com.acmutv.moviedoop.util.RecordParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import static com.acmutv.moviedoop.util.RecordParser.DELIMITER;
import static com.acmutv.moviedoop.util.RecordParser.RATING_FIELDS;

/**
 * The mapper for the {@link Query1_2} job.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class RatingsFilterMapper extends Mapper<Object, Text, LongWritable, Text> {

  /**
   * The movie rating threshold.
   */
  private Double ratingThreshold;

  /**
   * The starting date for movie rating.
   */
  private long startDate;

  /**
   * The movie id to emit.
   */
  private LongWritable movieId = new LongWritable();

  /**
   * The movie rating to emit.
   */
  private Text movieRating = new Text();

  /**
   * Configures the mapper.
   * @param ctx the job context.
   */
  protected void setup(Context ctx) {
    this.ratingThreshold = Double.valueOf(ctx.getConfiguration().get("ratingThreshold"));
    this.startDate = ctx.getConfiguration().getLong("startDate", Long.MIN_VALUE);
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
    long movieId = Long.valueOf(rating.get("movieId"));
    double score = Double.valueOf(rating.get("score"));
    long timestamp = Long.valueOf(rating.get("timestamp"));
    System.out.printf("# MAP # Input (%d,%f,%d)\n", movieId, score, timestamp);
    if (timestamp >= this.startDate && score >= this.ratingThreshold) {
      this.movieId.set(movieId);
      this.movieRating.set("R" + score);
      System.out.printf("# MAP # Write (%d,%f)\n", movieId, score);
      ctx.write(this.movieId, this.movieRating);
    }
  }
}