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
package com.acmutv.moviedoop.query2.map;

import com.acmutv.moviedoop.common.util.RecordParser;
import com.acmutv.moviedoop.query2.Query2_2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The mapper for jobs in: {@link Query2_2}.
 * It emits (movieId,rating) where rating is a score attributed with timestamp greater or equal to
 * the `movieRatingTimestampLowerBound`.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class RatingsAggregateMoviesAggregateCachedMapper extends Mapper<Object,Text,LongWritable,Text> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(RatingsAggregateMoviesAggregateCachedMapper.class);

  /**
   * The map movieId->(score,repetitions).
   */
  private Map<Long,Map<Double,Long>> movieIdToAggregateRatings = new HashMap<>();

  /**
   * The movie id to emit.
   */
  private LongWritable movieId = new LongWritable();

  /**
   * The tuple (score,repetitions) to emit.
   */
  private String tupla = new String();

  /**
   * The tuple (score,repetitions) to emit.
   */
  private Text tuple = new Text();

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
    Map<String,String> rating = RecordParser.parse(value.toString(), new String[] {"userId","movieId","score","timestamp"}, RecordParser.DELIMITER);

    long movieId = Long.valueOf(rating.get("movieId"));
    double score = Double.valueOf(rating.get("score"));
    this.movieIdToAggregateRatings.putIfAbsent(movieId, new HashMap<>());
    this.movieIdToAggregateRatings.get(movieId).compute(score, (k,v) -> (v == null) ? 1 : v + 1);
  }

  /**
   * Flushes the mapper.
   *
   * @param ctx the job context.
   */
  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    for (Long movieId : this.movieIdToAggregateRatings.keySet()) {
      this.movieId.set(movieId);
      this.tupla = "";
      for (Map.Entry<Double,Long> entry : this.movieIdToAggregateRatings.get(movieId).entrySet()) {
        long repetitions = entry.getValue();
        if (repetitions == 0) continue;
        double score = entry.getKey();
        if (this.tupla.equals(""))
          this.tupla += Double.toString(score) + "=" + Long.toString(repetitions);
        else
          this.tupla += "," + Double.toString(score) + "=" + Long.toString(repetitions);
      }
      ctx.write(this.movieId, new Text(this.tupla));
    }
  }
}
