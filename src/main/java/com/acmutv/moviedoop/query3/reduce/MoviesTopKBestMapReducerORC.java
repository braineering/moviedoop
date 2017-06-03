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

import com.acmutv.moviedoop.common.struct.BestMap;
import com.acmutv.moviedoop.query3.Query3_4;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

import java.io.IOException;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * The reducer for jobs in: {@link Query3_4}.
 * It emits the top-`moviesTopKSize` (movieId,avgRating).
 * It leverages BestMap.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class MoviesTopKBestMapReducerORC extends Reducer<NullWritable,OrcValue,NullWritable,OrcStruct> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(MoviesTopKBestMapReducerORC.class);

  /**
   * The ORC schema.
   */
  public static final TypeDescription ORC_SCHEMA = TypeDescription.fromString("struct<id:bigint,avgrating:double>");

  /**
   * The ORC tuple (movieId,avgrating) to emit.
   */
  private OrcStruct out = (OrcStruct) OrcStruct.createValue(ORC_SCHEMA);

  /**
   * The movieId to emit.
   */
  private LongWritable movieId = (LongWritable) out.getFieldValue(0);

  /**
   * The avgrating to emit.
   */
  private DoubleWritable avgrating = (DoubleWritable) out.getFieldValue(1);

  /**
   * The rank data structure.
   */
  private BestMap rank = new BestMap();

  /**
   * Configures the reducer.
   *
   * @param ctx the job context.
   */
  protected void setup(Context ctx) {
    int moviesTopKSize = Integer.valueOf(ctx.getConfiguration().get("moviedoop.topk.size"));
    this.rank.setMaxSize(moviesTopKSize);
    LOG.debug("[SETUP] moviedoop.topk.size: " + moviesTopKSize);
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
  public void reduce(NullWritable key, Iterable<OrcValue> values, Context ctx) throws IOException, InterruptedException {
    for (OrcValue orcValue : values) {
      String value = ((Text)((OrcStruct) orcValue.value).getFieldValue(0)).toString();
      System.out.printf("### RED ### in = %s\n", value);
      String pairs[] = value.split(",", -1);
      for (String pair : pairs) {
        String elem[] = pair.split("=", -1);
        long movieId = Long.valueOf(elem[0]);
        double score = Double.valueOf(elem[1]);
        this.rank.put(movieId, score);
      }
    }
  }

  /**
   * Flushes the reducer.
   *
   * @param ctx the job context.
   */
  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    for (Map.Entry<Long,Double> entry :
        this.rank.entrySet().stream().sorted((e1,e2)-> e2.getValue().compareTo(e1.getValue())).collect(Collectors.toList())) {
      this.movieId.set(entry.getKey());
      this.avgrating.set(entry.getValue());
      ctx.write(NullWritable.get(), this.out);
      System.out.printf("### RED ### out = (%d,%f)\n", this.movieId.get(), this.avgrating.get());
    }
  }

}
