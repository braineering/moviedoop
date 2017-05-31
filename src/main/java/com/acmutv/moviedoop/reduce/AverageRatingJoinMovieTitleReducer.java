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
package com.acmutv.moviedoop.reduce;

import com.acmutv.moviedoop.Query1_1;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The reducer for the {@link Query1_1} job.
 * It joins (movieId,rating) and (movieId,movieTitle), and emits (movieTitle,avgRating)
 * where avgRating is the average rating greater than or equal to `movieAverageRatingLowerBound`.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class AverageRatingJoinMovieTitleReducer extends Reducer<LongWritable, Text, Text, DoubleWritable> {

  /**
   * The lower bound for the movie average rating.
   */
  private double movieAverageRatingLowerBound;

  /**
   * The movie title to emit.
   */
  private Text movieTitle = new Text();

  /**
   * The movie average rating to emit.
   */
  private DoubleWritable movieAverageRating = new DoubleWritable();

  /**
   * Configures the reducer.
   *
   * @param ctx the job context.
   */
  protected void setup(Context ctx) {
    this.movieAverageRatingLowerBound =
        Double.valueOf(ctx.getConfiguration().get("moviedoop.average.rating.lb"));
  }

  /**
   * The reduction routine.
   *
   * @param key the input key.
   * @param values the input values.
   * @param ctx the context.
   * @throws IOException when the context cannot be written.
   * @throws InterruptedException when the context cannot be written.
   */
  public void reduce(LongWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {

    long num = 0L;
    double sum = 0.0;

    for (Text value : values) {
      if (value.charAt(0) == 'R') { // rating
        double rating = Double.valueOf(value.toString().substring(1));
        sum += rating;
        num++;
      } else if (value.charAt(0) == 'M') { // movie
        String movieTitle = value.toString().substring(1);
        this.movieTitle.set(movieTitle);
      } else {
        final String errmsg = String.format("Object %s is neither a rating nor a movie", value.toString());
        throw new IOException(errmsg);
      }
    }

    double avgRating = sum / num;

    if (avgRating >= this.movieAverageRatingLowerBound) {
      this.movieAverageRating.set(avgRating);
      ctx.write(this.movieTitle, this.movieAverageRating);
    }
  }

}
