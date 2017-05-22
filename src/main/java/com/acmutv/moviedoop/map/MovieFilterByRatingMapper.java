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

import com.acmutv.moviedoop.Query1;
import com.acmutv.moviedoop.util.DateParser;
import com.acmutv.moviedoop.util.RecordParser;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.Map;

/**
 * The mapper for the {@link Query1} job.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class MovieFilterByRatingMapper extends Mapper<Object,Text,Text,DoubleWritable> {

  /**
   * The input record attributes.
   */
  private static final String[] ATTRIBUTES = {"userId","movieId","title","genres","rating","timestamp"};

  /**
   * The input record delimiter.
   */
  private static final String DELIMITER = ",";

  /**
   * The movie rating threshold.
   */
  private Double ratingThreshold;

  /**
   * The starting date for movie rating.
   */
  private LocalDate startDate;

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
    this.ratingThreshold = Double.valueOf(ctx.getConfiguration().get("ratingThreshold"));
    this.startDate = DateParser.parse(ctx.getConfiguration().get("startDate"));
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
    Map<String,String> record = RecordParser.parse(value.toString(), ATTRIBUTES, DELIMITER);
    double rating = Double.valueOf(record.get("rating"));

    if (rating >= this.ratingThreshold) {
      String title = record.get("title");
      this.movieTitle.set(title);
      this.movieRating.set(rating);
      ctx.write(this.movieTitle, this.movieRating);
    }
  }
}
