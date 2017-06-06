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

import com.acmutv.moviedoop.query2.Query2_2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

import java.io.IOException;

/**
 * The reducer for the {@link Query2_2} job.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class AggregateGenresReducerORC extends Reducer<OrcKey, OrcValue, Text, Text> {

  private Text genreTitle = new Text();

  private Text genreStats = new Text();

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

    Text genreTitle = ((Text)((OrcStruct) key.key).getFieldValue(0));
    this.genreTitle.set(genreTitle);

    long occ = 0L;
    double avg = 0.0;
    double stdDev = 0.0;
    double sum = 0.0;
    double sumStd = 0.0;

    for (OrcValue orcValue : values) {
      String value = ((Text)((OrcStruct) orcValue.value).getFieldValue(0)).toString();
      int length = value.length();
      String[] tokens = value.split(",");
      for(String t : tokens) {
        String[] couple = t.split("=");
        double score = Double.parseDouble(couple[0]);
        long repetitions = Long.parseLong(couple[1]);
        occ += repetitions;
        double temp = score * repetitions;
        sum += temp;
        sumStd += ((score) * (score)) * repetitions;
      }
    }

    avg = sum / occ;
    stdDev = (sumStd - (occ * avg * avg)) / (occ - 1);
    stdDev = Math.sqrt(stdDev);

    this.genreStats.set(avg + "\t" + stdDev);

    ctx.write(this.genreTitle, this.genreStats);
  }
}
