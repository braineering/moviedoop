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
package com.acmutv.moviedoop.test.reduce;

import com.acmutv.moviedoop.common.util.RecordParser;
import com.acmutv.moviedoop.test.QuerySerializationText2Text2Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * The reducer for the {@link QuerySerializationText2Text2Text} job.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class RatingJoinMovieTitleCachedTextReducerText2Text extends Reducer<LongWritable,Text,NullWritable,Text> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(RatingJoinMovieTitleCachedTextReducerText2Text.class);

  /**
   * The cached map (movieId,movieTitle).
   */
  private Map<Long,String> movieIdToMovieTitle = new HashMap<>();

  /**
   * The tuple (movieId,movieTitle,rating,time) title to emit.
   */
  private Text tuple = new Text();

  /**
   * Configures the reducer.
   *
   * @param ctx the job context.
   */
  protected void setup(Context ctx) {
    try {
      for (URI uri : ctx.getCacheFiles()) {
        Path path = new Path(uri);
        BufferedReader br = new BufferedReader(
            new InputStreamReader(
                new FileInputStream(path.getName())));
        String line;
        while ((line = br.readLine()) != null) {
          Map<String,String> movie = RecordParser.parse(line, new String[] {"id","title","genres"}, RecordParser.ESCAPED_DELIMITER);
          long movieId = Long.valueOf(movie.get("id"));
          String movieTitle = movie.get("title");
          this.movieIdToMovieTitle.put(movieId, movieTitle);
        }
        br.close();
      }
    } catch (IOException exc) {
      LOG.error(exc.getMessage());
      exc.printStackTrace();
    }
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
  public void reduce(LongWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
    long movieId = key.get();
    String movieTitle = this.movieIdToMovieTitle.get(movieId);
    for (Text value : values) {
      Map<String,String> parsed = RecordParser.parse(value.toString(), new String[] {"rating","time"}, RecordParser.ESCAPED_DELIMITER);
      double rating = Double.valueOf(parsed.get("rating"));
      long time = Long.valueOf(parsed.get("time"));
      this.tuple.set(movieId + "," + movieTitle + "," + rating + "," + time);
      ctx.write(NullWritable.get(), this.tuple);
    }
  }

}
