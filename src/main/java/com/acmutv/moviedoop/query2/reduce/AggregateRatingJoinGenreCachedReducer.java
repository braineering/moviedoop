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

import com.acmutv.moviedoop.common.util.RecordParser;
import com.acmutv.moviedoop.query1.Query1_1;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
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
 * The reducer for the {@link Query1_1} job.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class AggregateRatingJoinGenreCachedReducer extends Reducer<LongWritable,Text,Text,DoubleWritable> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(AggregateRatingJoinGenreCachedReducer.class);

  /**
   * The cached map (movieId,movieTitle)
   */
  private Map<Long,Text> movieIdToGenres = new HashMap<>();

  /**
   *
   */
  private DoubleWritable score = new DoubleWritable();

  /**
   * The genre name to emit.
   */
  private Text genreTitle = new Text();

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
          Map<String,String> movie = RecordParser.parse(line, new String[] {"id","title","genres"},RecordParser.ESCAPED_DELIMITER);
          long movieId = Long.valueOf(movie.get("id"));
          String genres = String.valueOf(movie.get("genres"));
          if(!genres.equals("(no genres listed)"))
            this.movieIdToGenres.put(movieId,new Text(genres));
        }
      }
    } catch (IOException exc) {
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

    for (Text value : values) {
      String[] tokens = value.toString().split(",");
      double score = Double.parseDouble(tokens[0]);
      long repetitions = Long.parseLong(tokens[1]);
      for(int j=0; j<=repetitions; j++) {
        if (this.movieIdToGenres.containsKey(movieId)) {
          String[] genres = this.movieIdToGenres.get(movieId).toString().split("\\|");
          for (int i = 0; i < genres.length; i++) {
            this.genreTitle.set(genres[i]);
            this.score.set(score);
            ctx.write(genreTitle, this.score);
          }
        }
      }
    }
  }
}
