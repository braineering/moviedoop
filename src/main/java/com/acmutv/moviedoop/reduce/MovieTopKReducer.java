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

import com.acmutv.moviedoop.Query1;
import com.acmutv.moviedoop.Query3;
import com.acmutv.moviedoop.struct.BestMap;
import com.acmutv.moviedoop.util.DateParser;
import com.acmutv.moviedoop.util.RecordParser;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.acmutv.moviedoop.util.RecordParser.DELIMITER;
import static com.acmutv.moviedoop.util.RecordParser.RANK_FIELDS;

/**
 * The reducer for the {@link Query3} job.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class MovieTopKReducer extends Reducer<NullWritable,Text,NullWritable,Text> {

  /**
   * The rank size.
   */
  private int rankSize;

  /**
   * The rank data structure.
   */
  private BestMap rank = new BestMap();

  /**
   * The tuple (movieId,rating) to emit.
   */
  private Text tuple = new Text();

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
    this.rankSize = Integer.valueOf(ctx.getConfiguration().get("rankSize"));
    System.out.println("# [SETUP RED] # rankSize: " + this.rankSize);
    this.rank.setMaxSize(this.rankSize);

    for (Text value : values) {
      Map<String,String> record = RecordParser.parse(value.toString(), RANK_FIELDS, DELIMITER);
      System.out.println("# [RED] # Record: " + record);
      Long movieId = Long.valueOf(record.get("movieId"));
      Double rating = Double.valueOf(record.get("rating"));
      this.rank.put(movieId, rating);
      System.out.println("# [RED] # Rank: " + this.rank);
    }

    for (Map.Entry<Long,Double> entry :
        this.rank.entrySet().stream().sorted((e1,e2)-> e2.getValue().compareTo(e1.getValue())).collect(Collectors.toList())) {
      this.tuple.set(entry.getKey() + "," + entry.getValue());
      ctx.write(NullWritable.get(), this.tuple);
    }
  }

}
