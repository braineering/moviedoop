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

import com.acmutv.moviedoop.QueryTopK_1;
import com.acmutv.moviedoop.struct.BestMap;
import com.acmutv.moviedoop.util.RecordParser;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * The reducer for the {@link QueryTopK_1} job.
 * It emits the top-`moviesTopKSize` (movieId,avgRating).
 * It leverages TreeMap.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class MoviesTopKTreeMapReducer extends Reducer<NullWritable,Text,NullWritable,Text> {

  /**
   * The movies rank size.
   */
  private int moviesTopKSize;

  /**
   * The rank data structure.
   */
  private TreeMap<Double,Long> rank = new TreeMap<>();

  /**
   * The tuple (movieId,rating) to emit.
   */
  private Text tuple = new Text();

  /**
   * Configures the reducer.
   *
   * @param ctx the job context.
   */
  protected void setup(Context ctx) {
    this.moviesTopKSize = Integer.valueOf(ctx.getConfiguration().get("moviedoop.topk.size"));
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
  public void reduce(NullWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
    for (Text value : values) {
      Map<String,String> rankRecord = RecordParser.parse(value.toString(), new String[] {"movieId","score"}, ",");
      System.out.printf("### RED ### TopKBestMapReducer :: value: %s\n", value.toString());

      long movieId = Long.valueOf(rankRecord.get("movieId"));
      double score = Double.valueOf(rankRecord.get("score"));

      this.rank.put(score, movieId);

      if (this.rank.size() > this.moviesTopKSize) {
        this.rank.remove(this.rank.firstKey());
      }
    }

    for (Map.Entry<Double,Long> entry : this.rank.descendingMap().entrySet()) {
      this.tuple.set(entry.getValue() + "," + entry.getKey());
      ctx.write(NullWritable.get(), this.tuple);
    }
  }

}
