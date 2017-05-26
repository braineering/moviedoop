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

import com.acmutv.moviedoop.Query1_1;
import com.acmutv.moviedoop.QueryTopK;
import com.acmutv.moviedoop.util.DateParser;
import com.acmutv.moviedoop.util.RecordParser;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * The mapper for the {@link QueryTopK} job.
 * It emits (movieId,rating) where rating is a score attributed with timestamp greater or equal to
 * the `movieRatingTimestampLowerBound`.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class FilterRatingsByTimeIntervalMapper extends Mapper<Object,Text,LongWritable,DoubleWritable> {

  /**
   * The lower bound for the movie rating timestamp.
   */
  private long movieRatingTimestampLowerBound;

  /**
   * The upper bound for the movie rating timestamp.
   */
  private long movieRatingTimestampUpperBound;

  /**
   * The movie id to emit.
   */
  private LongWritable movieId = new LongWritable();

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
        DateParser.toSeconds(ctx.getConfiguration().get("movie.topk.rating.timestamp.lb"));
    this.movieRatingTimestampUpperBound =
        DateParser.toSeconds(ctx.getConfiguration().get("movie.topk.rating.timestamp.ub"));
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
    if (timestamp >= this.movieRatingTimestampLowerBound
        && timestamp <= this.movieRatingTimestampUpperBound) {
      long movieId = Long.valueOf(rating.get("movieId"));
      double score = Double.valueOf(rating.get("score"));
      this.movieId.set(movieId);
      this.movieRating.set(score);
      ctx.write(this.movieId, this.movieRating);
    }
  }
}
