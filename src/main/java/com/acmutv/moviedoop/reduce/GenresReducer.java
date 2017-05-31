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

import com.acmutv.moviedoop.Query2_2;
import com.acmutv.moviedoop.model.GenreWritable;
import com.acmutv.moviedoop.model.MovieWritable;
import com.acmutv.moviedoop.model.RatingsWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The reducer for the {@link Query2_2} job.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class GenresReducer extends Reducer<Text, DoubleWritable, Text, Text> {

  /**
   * The stdDev of genre's rating
   */
  private GenreWritable genre = new GenreWritable();

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
    double score = 0.0;
    long occ = 0L;
    double avg = 0.0;
    double stdDev = 0.0;
    double newAvg = 0.0;
    double newStdDev = 0.0;
    this.genre.occurrences = 0L;

    for (DoubleWritable value : values) {
      if (this.genre.occurrences == 0L) {
        this.genre.average = value.get();
        this.genre.stdDev = 0.0;
        this.genre.occurrences  = 1L;
      }
      else {
        score = value.get();
        occ = this.genre.occurrences;
        avg = this.genre.average;
        stdDev = this.genre.stdDev;

        newAvg = ((avg * occ) + score) / (occ + 1);
        occ += 1L;
        newStdDev = ((occ-2) * Math.pow(stdDev,2.0) + (score - newAvg)*(score - avg))/(occ-1);

        this.genre.occurrences = occ;
        this.genre.average = newAvg;
        this.genre.stdDev = newStdDev;
      }
    }
    ctx.write(genreTitle, new Text(genre.printRatings()));
  }
}
