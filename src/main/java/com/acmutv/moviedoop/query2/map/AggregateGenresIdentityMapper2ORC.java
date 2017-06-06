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
package com.acmutv.moviedoop.query2.map;

import com.acmutv.moviedoop.query2.Query2_2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

import java.io.IOException;

/**
 * The mapper for the {@link Query2_2} job.
 * It emits (movieId,'M'movieTitle).
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class AggregateGenresIdentityMapper2ORC extends Mapper<Object, OrcStruct, OrcKey, OrcValue> {

  /**
   * The ORC schema for key.
   */
  public static final TypeDescription ORC_SCHEMA_KEY = TypeDescription.fromString("struct<genre:string>");

  /**
   * The ORC schema for value.
   */
  public static final TypeDescription ORC_SCHEMA_VALUE = TypeDescription.fromString("struct<ratings:string>");

  /**
   * The key ORC wrapper
   */
  private OrcKey keywrapper = new OrcKey();

  /**
   * The value ORC wrapper.
   */
  private OrcValue valuewrapper = new OrcValue();

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
  private Text genre = (Text) keyStruct.getFieldValue(0);

  /**
   * The tuple {rating=repetitions,...,rating=repetitions} to emit.
   */
  private Text ratings = (Text) valueStruct.getFieldValue(0);

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
    String genre = String.valueOf(value.getFieldValue(0).toString());
    String ratings = String.valueOf(value.getFieldValue(0).toString());
    this.genre.set(genre);
    this.ratings.set(ratings);
    this.keywrapper.key = this.keyStruct;
    this.valuewrapper.value = this.valueStruct;
    ctx.write(this.keywrapper,this.valuewrapper);
  }
}
