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
package com.acmutv.moviedoop.query3.map;

import com.acmutv.moviedoop.common.util.DateParser;
import com.acmutv.moviedoop.common.util.RecordParser;
import com.acmutv.moviedoop.query3.Query3_1;
import com.acmutv.moviedoop.query3.Query3_2;
import com.acmutv.moviedoop.query3.Query3_3;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The mapper for jobs in: {@link Query3_3}.
 * It emits N(movieId,rating) where N=(1|2) and rating is a score attributed with timestamp within
 * [`movieRatingTimestampLowerBound1`,`movieRatingTimestampUpperBound1`] or within
 * [`movieRatingTimestampLowerBound2`,`movieRatingTimestampUpperBound2`]
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class FilterRatingsBy2TimeIntervalAndAggregate2Mapper extends Mapper<Object,Text,LongWritable,Text> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(FilterRatingsBy2TimeIntervalAndAggregate2Mapper.class);

  /**
   * The lower bound for the movie rating timestamp (1).
   */
  private long movieRatingTimestampLowerBound1;

  /**
   * The upper bound for the movie rating timestamp (1).
   */
  private long movieRatingTimestampUpperBound1;

  /**
   * The lower bound for the movie rating timestamp (2).
   */
  private long movieRatingTimestampLowerBound2;

  /**
   * The upper bound for the movie rating timestamp (2).
   */
  private long movieRatingTimestampUpperBound2;

  /**
   * The map movieId->(score,repetitions) (1).
   */
  private Map<Long,Map<Double,Long>> movieIdToAggregateRatings_1 = new HashMap<>();

  /**
   * The map movieId->(score,repetitions) (2).
   */
  private Map<Long,Map<Double,Long>> movieIdToAggregateRatings_2 = new HashMap<>();

  /**
   * The map movieId->(score,repetitions) (1,2).
   */
  private Map<Long,Map<Double,Long>> movieIdToAggregateRatings_1_2 = new HashMap<>();

  /**
   * The movie id to emit.
   */
  private LongWritable movieId = new LongWritable();

  /**
   * The tuple (N,score) to emit.
   */
  private Text tuple = new Text();

  /**
   * Configures the mapper.
   * @param ctx the job context.
   */
  protected void setup(Context ctx) {
    this.movieRatingTimestampLowerBound1 =
        DateParser.toSeconds(ctx.getConfiguration().get("moviedoop.average.rating.timestamp.lb.1"));
    this.movieRatingTimestampUpperBound1 =
        DateParser.toSeconds(ctx.getConfiguration().get("moviedoop.average.rating.timestamp.ub.1"));
    this.movieRatingTimestampLowerBound2 =
        DateParser.toSeconds(ctx.getConfiguration().get("moviedoop.average.rating.timestamp.lb.2"));
    this.movieRatingTimestampUpperBound2 =
        DateParser.toSeconds(ctx.getConfiguration().get("moviedoop.average.rating.timestamp.ub.2"));
    LOG.debug("[SETUP] moviedoop.average.rating.timestamp.lb.1: " + this.movieRatingTimestampLowerBound1);
    LOG.debug("[SETUP] moviedoop.average.rating.timestamp.ub.1: " + this.movieRatingTimestampUpperBound1);
    LOG.debug("[SETUP] moviedoop.average.rating.timestamp.lb.2: " + this.movieRatingTimestampLowerBound2);
    LOG.debug("[SETUP] moviedoop.average.rating.timestamp.ub.2: " + this.movieRatingTimestampUpperBound2);
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
    if (timestamp >= this.movieRatingTimestampLowerBound1
        && timestamp <= this.movieRatingTimestampUpperBound1
        && timestamp >= this.movieRatingTimestampLowerBound2
        && timestamp <= this.movieRatingTimestampUpperBound2) {
      long movieId = Long.valueOf(rating.get("movieId"));
      double score = Double.valueOf(rating.get("score"));
      this.movieIdToAggregateRatings_1_2.putIfAbsent(movieId, new HashMap<>());
      this.movieIdToAggregateRatings_1_2.get(movieId).compute(score, (k,v) -> (v == null) ? 1 : v + 1);
    } else if (timestamp >= this.movieRatingTimestampLowerBound1
        && timestamp <= this.movieRatingTimestampUpperBound1) {
      long movieId = Long.valueOf(rating.get("movieId"));
      double score = Double.valueOf(rating.get("score"));
      this.movieIdToAggregateRatings_1.putIfAbsent(movieId, new HashMap<>());
      this.movieIdToAggregateRatings_1.get(movieId).compute(score, (k,v) -> (v == null) ? 1 : v + 1);
    } else if (timestamp >= this.movieRatingTimestampLowerBound2
        && timestamp <= this.movieRatingTimestampUpperBound2) {
      long movieId = Long.valueOf(rating.get("movieId"));
      double score = Double.valueOf(rating.get("score"));
      this.movieIdToAggregateRatings_2.putIfAbsent(movieId, new HashMap<>());
      this.movieIdToAggregateRatings_2.get(movieId).compute(score, (k,v) -> (v == null) ? 1 : v + 1);
    }
  }

  /**
   * Flushes the mapper.
   *
   * @param ctx the job context.
   */
  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    for (Long movieId : this.movieIdToAggregateRatings_1_2.keySet()) {
      this.movieId.set(movieId);
      String report = this.movieIdToAggregateRatings_1_2.get(movieId).toString().trim();
      report = report.substring(1, report.length() - 1);
      this.tuple.set("1;2:" + report);
      ctx.write(this.movieId, this.tuple);
    }

    for (Long movieId : this.movieIdToAggregateRatings_1.keySet()) {
      this.movieId.set(movieId);
      String report = this.movieIdToAggregateRatings_1.get(movieId).toString().trim();
      report = report.substring(1, report.length() - 1);
      this.tuple.set("1:" + report);
      ctx.write(this.movieId, this.tuple);
    }

    for (Long movieId : this.movieIdToAggregateRatings_2.keySet()) {
      this.movieId.set(movieId);
      String report = this.movieIdToAggregateRatings_2.get(movieId).toString().trim();
      report = report.substring(1, report.length() - 1);
      this.tuple.set("2:" + report);
      ctx.write(this.movieId, this.tuple);
    }
  }
}
