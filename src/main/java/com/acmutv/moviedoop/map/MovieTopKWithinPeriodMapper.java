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

import com.acmutv.moviedoop.Query3;
import com.acmutv.moviedoop.struct.BestMap;
import com.acmutv.moviedoop.util.DateParser;
import com.acmutv.moviedoop.util.RecordParser;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.TreeMap;

import static com.acmutv.moviedoop.util.RecordParser.RATING_FIELDS;
import static com.acmutv.moviedoop.util.RecordParser.DELIMITER;

/**
 * The mapper for the {@link Query3} job.
 * It produces the top K movies, expressed by tuples (movieId,rating), rated within the interval
 * {@code startDate} and {@code endDate}.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class MovieTopKWithinPeriodMapper extends Mapper<Object,Text,NullWritable,Text> {

  /**
   * The rank size.
   */
  private int rankSize;

  /**
   * The start date.
   */
  private LocalDateTime startDate;

  /**
   * The end date.
   */
  private LocalDateTime endDate;

  /**
   * The rank data structure.
   */
  private BestMap rank = new BestMap();

  /**
   * The tuple (movieId,rating) to emit.
   */
  private Text tuple = new Text();

  /**
   * Configures the mapper.
   * @param ctx the job context.
   */
  protected void setup(Context ctx) {
    this.rankSize = Integer.valueOf(ctx.getConfiguration().get("rankSize"));
    this.startDate = DateParser.parse(ctx.getConfiguration().get("startDate1"));
    this.endDate = DateParser.parse(ctx.getConfiguration().get("endDate1"));
    System.out.println("# [SETUP MAP] # rankSize: " + this.rankSize);
    System.out.println("# [SETUP MAP] # startDate: " + this.startDate);
    System.out.println("# [SETUP MAP] # endDate: " + this.endDate);
    this.rank.setMaxSize(this.rankSize);
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
    Map<String,String> record = RecordParser.parse(value.toString(), RATING_FIELDS, DELIMITER);
    System.out.println("# [MAP] # Record: " + record);

    LocalDateTime timestamp = DateParser.parse(record.get("timestamp"));

    if (timestamp.isAfter(this.startDate) && timestamp.isBefore(this.endDate)) {
      Long movieId = Long.valueOf(record.get("movieId"));
      Double rating = Double.valueOf(record.get("rating"));
      this.rank.put(movieId, rating);
    }
  }

  /**
   * Flushes the mapper.
   * @param ctx the job context.
   */
  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    for (Map.Entry<Long,Double> entry : this.rank.entrySet()) {
      this.tuple.set(entry.getKey() + "," + entry.getValue());
      ctx.write(NullWritable.get(), this.tuple);
    }
  }
}
