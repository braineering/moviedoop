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

import com.acmutv.moviedoop.query3.Query3_4;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

import java.io.IOException;

/**
 * The reducer for jobs in: {@link Query3_4}.
 * It emits (movieId,avgRating) where avgRating is the average rating.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class AverageRating2AndAggregate2ReducerORC extends Reducer<OrcKey,OrcValue,NullWritable,OrcStruct> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(AverageRating2AndAggregate2ReducerORC.class);

  /**
   * The multiple outputs.
   */
  private MultipleOutputs<NullWritable,OrcStruct> mos;

  /**
   * The ORC schema.
   */
  public static final TypeDescription ORC_SCHEMA = TypeDescription.fromString("struct<id:bigint,avgrating:double>");

  /**
   * The ORC tuple (movieId,avgrating) to emit.
   */
  private OrcStruct tuple = (OrcStruct) OrcStruct.createValue(ORC_SCHEMA);

  /**
   * The movieId to emit.
   */
  private LongWritable movieId = (LongWritable) tuple.getFieldValue(0);

  /**
   * The avgrating to emit.
   */
  private DoubleWritable avgrating = (DoubleWritable) tuple.getFieldValue(1);

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
  public void reduce(OrcKey key, Iterable<OrcValue> values, Context ctx) throws IOException, InterruptedException {
    long num1 = 0L;
    double sum1 = 0.0;

    long num2 = 0L;
    double sum2 = 0.0;

    for (OrcValue orcValue : values) {
      String value = ((Text)((OrcStruct) orcValue.value).getFieldValue(0)).toString();
      String parts[] = value.split(":");
      String header[] = parts[0].split(";",-1);
      boolean is1 = header.length >= 1 && header[0].equals("1");
      boolean is2 = header.length >= 2 && header[1].equals("2");
      String pairs[] = parts[1].split(",", -1);
      String elems[][] = new String[pairs.length][2];
      for (int i = 0; i < pairs.length; i++) {
        String tmp[] = pairs[i].split("=", -1);
        elems[i][0] = tmp[0];
        elems[i][1] = tmp[1];
      }

      if (is1) {
        for (String elem[] : elems) {
          double score = Double.valueOf(elem[0]);
          long repetitions = Long.valueOf(elem[1]);
          sum1 += (score * repetitions);
          num1 += repetitions;
        }
      }

      if (is2) {
        for (String elem[] : elems) {
          double score = Double.valueOf(elem[0]);
          long repetitions = Long.valueOf(elem[1]);
          sum2 += (score * repetitions);
          num2 += repetitions;
        }
      }
    }

    double avgScore1 = sum1 / num1;
    double avgScore2 = sum2 / num2;

    long movieId = ((LongWritable) ((OrcStruct) key.key).getFieldValue(0)).get();

    if (num1 > 0) {
      this.movieId.set(movieId);
      this.avgrating.set(avgScore1);
      this.mos.write("1", NullWritable.get(), this.tuple);
    }

    if (num2 > 0) {
      this.movieId.set(movieId);
      this.avgrating.set(avgScore2);
      this.mos.write("2", NullWritable.get(), this.tuple);
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
