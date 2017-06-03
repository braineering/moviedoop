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
package com.acmutv.moviedoop.query3.map;

import com.acmutv.moviedoop.common.struct.BestMap;
import com.acmutv.moviedoop.query3.Query3_4;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

import java.io.IOException;

/**
 * The mapper for jobs in: {@link Query3_4}.
 * It emits the top-`moviesTopKSize` (movieId,avgRating).
 * It leverages BestMap.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class MoviesTopKBestMapMapperORC extends Mapper<Object,OrcStruct,NullWritable,OrcValue> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(MoviesTopKBestMapMapperORC.class);

  /**
   * The ORC schema for key.
   */
  //public static final TypeDescription ORC_SCHEMA_KEY = TypeDescription.fromString("struct<id:bigint>");

  /**
   * The ORC schema for value.
   */
  public static final TypeDescription ORC_SCHEMA_VALUE = TypeDescription.fromString("struct<topk:string>");

  /**
   * The key ORC wrapper
   */
  //private OrcKey keywrapper = new OrcKey();

  /**
   * The value ORC wrapper.
   */
  private OrcValue valuewrapper = new OrcValue();

  /**
   * The ORC struct for key.
   */
  //private OrcStruct keyStruct = (OrcStruct) OrcStruct.createValue(ORC_SCHEMA_KEY);

  /**
   * The ORC struct for value.
   */
  private OrcStruct valueStruct = (OrcStruct) OrcStruct.createValue(ORC_SCHEMA_VALUE);

  /**
   * The rank data structure.
   */
  private BestMap rank = new BestMap();

  /**
   * The tuple {movieId=avgrating,...,movieId=avgrating} to emit.
   */
  private Text tuple = (Text) valueStruct.getFieldValue(0);

  /**
   * Configures the mapper.
   *
   * @param ctx the job context.
   */
  protected void setup(Context ctx) {
    int moviesTopKSize = Integer.valueOf(ctx.getConfiguration().get("moviedoop.topk.size"));
    this.rank.setMaxSize(moviesTopKSize);
    LOG.debug("[SETUP] moviedoop.topk.size: " + moviesTopKSize);
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
  public void map(Object key, OrcStruct value, Context ctx) throws IOException, InterruptedException {
    long movieId = Long.valueOf(value.getFieldValue(0).toString());
    double rating = Double.valueOf(value.getFieldValue(1).toString());

    this.rank.put(movieId, rating);
  }

  /**
   * Flushes the mapper.
   *
   * @param ctx the job context.
   */
  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    String report = this.rank.toString().trim();
    report = report.substring(1, report.length() - 1);
    this.tuple.set(report);
    this.valuewrapper.value = valueStruct;
    ctx.write(NullWritable.get(), this.valuewrapper);
  }
}
