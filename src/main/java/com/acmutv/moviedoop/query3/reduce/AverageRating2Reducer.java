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
package com.acmutv.moviedoop.query3.reduce;

import com.acmutv.moviedoop.query3.Query3_1;
import com.acmutv.moviedoop.query3.Query3_2;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * The reducer for jobs in: {@link Query3_1}, {@link Query3_2}.
 * It emits (movieId,avgRating) where avgRating is the average rating.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class AverageRating2Reducer extends Reducer<LongWritable,Text,NullWritable,Text> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(AverageRating2Reducer.class);

  /**
   * The null writable value.
   */
  private static final NullWritable NULL = NullWritable.get();

  /**
   * The multiple outputs.
   */
  private MultipleOutputs<NullWritable,Text> mos;

  /**
   * The tuple (movieId,avgrating) to emit.
   */
  private Text tuple = new Text();

  /**
   * Configures the reducer.
   *
   * @param ctx the job context.
   */
  protected void setup(Context ctx) {
    this.mos = new MultipleOutputs<>(ctx);
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
    long num1 = 0L;
    double sum1 = 0.0;

    long num2 = 0L;
    double sum2 = 0.0;

    for (Text value : values) {
      String parts[] = value.toString().split(":");
      String header[] = parts[0].split(";",-1);
      boolean is1 = header.length >= 1 && header[0].equals("1");
      boolean is2 = header.length >= 2 && header[1].equals("2");
      double rating = Double.valueOf(parts[1]);
      if (is1) {
        sum1 += rating;
        num1++;
      }

      if (is2) {
        sum2 += rating;
        num2++;
      }
    }

    double avgScore1 = sum1 / num1;
    double avgScore2 = sum2 / num2;

    long movieId = key.get();

    if (num1 > 0) {
      this.tuple.set(movieId + "," + avgScore1);
      this.mos.write("1", NULL, this.tuple);
    }

    if (num2 > 0) {
      this.tuple.set(movieId + "," + avgScore2);
      this.mos.write("2", NULL, this.tuple);
    }
  }

  /**
   * Flushes the reducer.
   *
   * @param ctx the job context.
   */
  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    this.mos.close();
  }

}
