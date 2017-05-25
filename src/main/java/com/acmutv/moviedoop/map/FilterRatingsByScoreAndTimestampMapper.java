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
import com.acmutv.moviedoop.util.RecordParser;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * The mapper for the {@link Query1_1} job.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class FilterRatingsByScoreAndTimestampMapper extends Mapper<Object,Text,LongWritable,DoubleWritable> {

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
  private DoubleWritable movieRating = new DoubleWritable();

  /**
   * Configures the mapper.
   * @param ctx the job context.
   */
  protected void setup(Context ctx) {
    this.ratingThreshold = ctx.getConfiguration().getDouble("ratingThreshold", Double.MIN_VALUE);
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
    double score = Double.valueOf(rating.get("score"));
    long timestamp = Long.valueOf(rating.get("timestamp"));

    if (timestamp >= this.startDate && score >= this.ratingThreshold) {
      Long movieId = Long.valueOf(rating.get("movieId"));
      this.movieId.set(movieId);
      this.movieRating.set(score);
      ctx.write(this.movieId, this.movieRating);
    }
  }
}
