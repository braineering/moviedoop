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

import com.acmutv.moviedoop.common.model.GenreWritable;
import com.acmutv.moviedoop.common.model.MovieWritable;
import com.acmutv.moviedoop.common.model.RatingsWritable;
import com.acmutv.moviedoop.query2.Query2_1;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The reducer for the {@link Query2_1} job.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class RatingsJoinGenresReducer extends Reducer<LongWritable, Text, Text, Text> {

  /**
   * The map (movieId, movieRating) for the inner join.
   */
  private Map<Long, Double> ratings = new HashMap<>();

  /**
   * The map (movieId,genreTitle) for the inner join.
   */
  private Map<Long, Text> movieToGenres = new HashMap<>();

  /**
   *
   */
  private Map<Text,GenreWritable> genres = new HashMap<>();

  /**
   *
   */
  private Text temp = new Text();

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



    this.ratings.clear();
    this.genres.clear();

    for (Text value : values) {
      if (value.toString().startsWith("R")) {
        long movieId = key.get();
        double rating = Double.valueOf(value.toString().substring(1));
        if (this.ratings.getOrDefault(movieId, Double.MIN_VALUE).compareTo(rating) < 0) {
          this.ratings.put(movieId, rating);
        }
      } else if (value.toString().startsWith("G")) {
        long movieId = key.get();
        String genres = value.toString().substring(1);
        this.movieToGenres.put(movieId,new Text(genres));
      } else {
        final String errmsg = String.format("Object is neither %s nor %s",
            RatingsWritable.class.getName(), MovieWritable.class.getName());
        throw new IOException(errmsg);
      }
    }

    for (Map.Entry<Long, Double> entryRating : this.ratings.entrySet()) {

      long movieId = entryRating.getKey();
      double score = entryRating.getValue();

      if(this.movieToGenres.containsKey(movieId)) {
        String[] genres = this.movieToGenres.get(movieId).toString().split("\\|");
        for (int i = 0; i < genres.length; i++) {
          String genre = genres[i];
          if (!this.genres.containsKey(new Text(genre))) {
            GenreWritable newGenre = new GenreWritable();
            newGenre.title = genre;
            newGenre.average = score;
            newGenre.stdDev = 0.0;
            newGenre.occurrences = 1L;
            this.genres.put(new Text(genre), newGenre);
            //ctx.write(new Text(genre.toString()), new Text(newGenre.toString()));
          } else {
            long occ = this.genres.get(genre).occurrences;
            double avg = this.genres.get(genre).average;
            double stdDev = this.genres.get(genre).stdDev;

            double newAvg = ((avg * occ) + score) / (occ + 1);
            occ += 1L;
            double newStdDev = ((occ-2) * Math.pow(stdDev,2.0) + (score - newAvg)*(score - avg))/(occ-1);

            GenreWritable update = new GenreWritable();
            update.title = genre;
            update.occurrences = occ;
            update.average = newAvg;
            update.stdDev = newStdDev;
            this.temp.set(genre);
            this.genres.replace(temp, update);
            //ctx.write(temp, new Text(this.genres.get(genre).toString()));
          }
        }
      }
    }

    for (Map.Entry<Text, GenreWritable> entryGenres : this.genres.entrySet()) {
      ctx.write(entryGenres.getKey(), new Text(entryGenres.getValue().toString()));
    }

  }

}
