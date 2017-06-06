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
package com.acmutv.moviedoop.query2.reduce;

import com.acmutv.moviedoop.common.model.GenreWritable;
import com.acmutv.moviedoop.query2.Query2_1;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * The reducer for the {@link Query2_1} job.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class
GenresReducer extends Reducer<Text, DoubleWritable, Text, Text> {

  /**
   * The reduction routine.
   *
   * @param key the input key.
   * @param values the input values.
   * @param ctx the context.
   * @throws IOException when the context cannot be written.
   * @throws InterruptedException when the context cannot be written.
   */
  public void reduce(Text key, Iterable<DoubleWritable> values, Context ctx) throws IOException, InterruptedException {

    Text genreTitle = key;
    long occ = 0L;
    double avg = 0.0;
    double stdDev = 0.0;

    double sum = 0.0;
    double temp = 0.0;

    double sumStd = 0.0;

    for (DoubleWritable value : values) {
      occ += 1L;
      temp = value.get();
      sum += temp;
      sumStd += temp * temp;
    }
    avg = sum / occ;

    stdDev = (sumStd - (occ * avg * avg)) / (occ - 1);

    stdDev = Math.sqrt(stdDev);

    ctx.write(genreTitle, new Text( Double.toString(avg) + "  "+Double.toString(stdDev)));
  }
}
