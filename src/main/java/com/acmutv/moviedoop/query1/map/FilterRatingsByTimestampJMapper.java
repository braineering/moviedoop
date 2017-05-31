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

import com.acmutv.moviedoop.query1.Query1_1;
import com.acmutv.moviedoop.common.util.DateParser;
import com.acmutv.moviedoop.common.util.RecordParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * The mapper for jobs in: {@link Query1_1}.
 * It emits (movieId,'R'rating) where rating is a score attributed with timestamp greater or equal to
 * the `movieRatingTimestampLowerBound`.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class FilterRatingsByTimestampJMapper extends Mapper<Object, Text, LongWritable, Text> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(FilterRatingsByTimestampJMapper.class);

  /**
   * The lower bound for the movie rating timestamp.
   */
  private long movieRatingTimestampLowerBound;

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
    this.movieRatingTimestampLowerBound =
        DateParser.toSeconds(ctx.getConfiguration().get("moviedoop.average.rating.timestamp.lb"));
    LOG.debug("[SETUP] moviedoop.average.rating.timestamp.lb: " + this.movieRatingTimestampLowerBound);
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
      this.movieId.set(movieId);
      this.movieRating.set("R" + score);
      ctx.write(this.movieId, this.movieRating);
    }
  }
}
