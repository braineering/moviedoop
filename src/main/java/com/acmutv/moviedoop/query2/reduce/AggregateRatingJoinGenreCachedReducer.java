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
import org.apache.hadoop.mapreduce.Mapper;
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
public class AggregateRatingJoinGenreCachedReducer extends Reducer<LongWritable,Text,Text,Text> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(AggregateRatingJoinGenreCachedReducer.class);

  /**
   * The cached map (movieId,movieTitle)
   */
  private Map<Long,Text> movieIdToGenres = new HashMap<>();

  /**
   * The genre rating to emit.
   */
  private DoubleWritable genreRating = new DoubleWritable();

  /**
   *
   */
  private Map<Text,Map<Double,Long>> allRatingsForAGenre = new HashMap<>();

  /**
   * The genre name to emit.
   */
  private Text genreTitle = new Text();

  /**
   * The genre Id to emit.
   */
  private Text genreId = new Text();


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

    Map<Double,Long> allRatingsForAMovie = new HashMap<>();

    for (Text value : values) {
      Map<String,String> parsed = RecordParser.parse(value.toString(), new String[]{"score","repetitions"}, RecordParser.DELIMITER);
      double score = Double.valueOf(parsed.get("score"));
      long repetitions = Long.valueOf(parsed.get("repetitions"));
      allRatingsForAMovie.put(score,repetitions);
    }

    if (this.movieIdToGenres.containsKey(movieId)) {
      String[] genres = this.movieIdToGenres.get(movieId).toString().split("\\|");
      for (int i = 0; i < genres.length; i++) {
        this.genreTitle.set(genres[i]);
        this.allRatingsForAGenre.put(genreTitle,allRatingsForAMovie);
        ctx.write(genreTitle,new Text(allRatingsForAMovie.toString()));
      }
    }
  }

  /**
   * Flushes the mapper.
   *
   * @param ctx the job context.
   */
 /* protected void cleanup(Reducer.Context ctx) throws IOException, InterruptedException {
    for (Text genreId : this.allRatingsForAGenre.keySet()) {
      this.genreId.set(genreId);

      long occ = 0L;
      double sum = 0.0;
      double sumStd = 0.0;
      double avg = 0.0;
      double stdDev = 0.0;

      String value = " - ";

      for(Map.Entry<Double,Long> entry : this.allRatingsForAGenre.get(genreId).entrySet()) {
        long repetitions = entry.getValue();
        if(repetitions == 0) continue;
        //occ += repetitions;
        double score = entry.getKey();
        /*sum += score * repetitions;
        sumStd += ((score * repetitions) * (score * repetitions));*/
     /*   value += "["+Long.toString(repetitions)+";"+Double.toString(score)+"] - ";
      }

      ctx.write(this.genreId,new Text(value));

      /*
       Double sumq = 0.0;

    for (DoubleWritable value : values) {
      occ += 1L;
      temp = value.get();
      sum += temp;
      sumq += temp * temp;
    }
    avg = sum / occ;

    stdDev = (sumq - (occ * avg * avg)) / (occ - 1);

    stdDev = Math.sqrt(stdDev);
        */


      /*this.movieId.set(movieId);
      for (Map.Entry<Double,Long> entry : this.movieIdToAggregateRatings.get(movieId).entrySet()) {
        long repetitions = entry.getValue();
        if (repetitions == 0) continue;
        double score = entry.getKey();
        this.tuple.set(score + "," + repetitions);
        ctx.write(this.movieId, this.tuple);
      }*/
   /* }
  }*/

}
