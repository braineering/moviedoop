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
package com.acmutv.moviedoop.test.map;

import com.acmutv.moviedoop.test.QuerySerializationOrc2Orc2Orc;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

import java.io.IOException;

/**
 * The identity mapper for the {@link QuerySerializationOrc2Orc2Orc} job.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class IdentityMapperOrc2Orc extends Mapper<Object,OrcStruct,OrcKey,OrcValue> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(IdentityMapperOrc2Orc.class);

  /**
   * The ORC schema for key.
   */
  public static final TypeDescription ORC_SCHEMA_KEY = TypeDescription.fromString("struct<id:bigint>");

  /**
   * The ORC schema for value.
   */
  public static final TypeDescription ORC_SCHEMA_VALUE = TypeDescription.fromString("struct<rating:double,time:bigint>");

  /**
   * The ORC struct for key.
   */
  private OrcStruct keyStruct = (OrcStruct) OrcStruct.createValue(ORC_SCHEMA_KEY);

  /**
   * The ORC struct for value.
   */
  private OrcStruct valueStruct = (OrcStruct) OrcStruct.createValue(ORC_SCHEMA_VALUE);

  /**
   * The movieId to emit.
   */
  private LongWritable movieId = (LongWritable) keyStruct.getFieldValue(0);

  /**
   * The movie rating to emit.
   */
  private DoubleWritable rating = (DoubleWritable) valueStruct.getFieldValue(0);

  /**
   * The movie rating timestamp to emit.
   */
  private LongWritable time = (LongWritable) valueStruct.getFieldValue(1);

  /**
   * The key ORC wrapper
   */
  private OrcKey keywrapper = new OrcKey();

  /**
   * The value ORC wrapper.
   */
  private OrcValue valuewrapper = new OrcValue();

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
    long movieId = Long.valueOf(value.getFieldValue(1).toString());
    double rating = Double.valueOf(value.getFieldValue(2).toString());
    long time = Long.valueOf(value.getFieldValue(3).toString());
    this.movieId.set(movieId);
    this.rating.set(rating);
    this.time.set(time);
    this.keywrapper.key = this.keyStruct;
    this.valuewrapper.value = this.valueStruct;
    ctx.write(this.keywrapper, this.valuewrapper);
  }
}
