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

import com.acmutv.moviedoop.test.QueryTopK_2;
import com.acmutv.moviedoop.common.struct.BestMap;
import com.acmutv.moviedoop.common.util.RecordParser;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The reducer for the {@link QueryTopK_2} job.
 * It emits the top-`moviesTopKSize` (movieId,avgRating).
 * It leverages BestMap.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class MoviesTopKBestMapReducer extends Reducer<NullWritable,Text,NullWritable,Text> {

  /**
   * The movies rank size.
   */
  private int moviesTopKSize;

  /**
   * The rank data structure.
   */
  private BestMap rank = new BestMap();

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
    this.rank.setMaxSize(this.moviesTopKSize);
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

      long movieId = Long.valueOf(rankRecord.get("movieId"));
      double score = Double.valueOf(rankRecord.get("score"));

      this.rank.put(movieId, score);
    }

    for (Map.Entry<Long,Double> entry :
        this.rank.entrySet().stream().sorted((e1,e2)-> e2.getValue().compareTo(e1.getValue())).collect(Collectors.toList())) {
      this.tuple.set(entry.getKey() + "," + entry.getValue());
      ctx.write(NullWritable.get(), this.tuple);
    }
  }

}
