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
package com.acmutv.moviedoop.map;

import com.acmutv.moviedoop.Query3_1;
import com.acmutv.moviedoop.util.RecordParser;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * The mapper for the {@link Query3_1} job.
 * It emits (movieId,rating) where rating is a score attributed with timestamp greater or equal to
 * the `movieRatingTimestampLowerBound`.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class RankComparisonMapper extends Mapper<LongWritable,Text,NullWritable,Text> {

  /**
   * The map between movieId and movie top-k rankin (rank position and score).
   */
  private Map<Long,String> movieIdToMovieTopKPositionAndScore = new HashMap<>();

  /**
   * The map between movieId and movie title.
   */
  private Map<Long,String> movieIdToMovieTitle = new HashMap<>();

  /**
   * The tuple (topKPosition,movieId,rankPosition,delta) to emit.
   */
  private Text tuple = new Text();

  /**
   * Configures the mapper.
   * @param ctx the job context.
   */
  protected void setup(Context ctx) {
    String pathMovies = ctx.getConfiguration().get("moviedoop.path.movies");
    String pathTopK = ctx.getConfiguration().get("moviedoop.path.topk");
    System.out.printf("### MAP ### paths lookup: (%s) (%s)\n", pathMovies, pathTopK);
    try {
      for (URI uri : ctx.getCacheFiles()) {
        Path path = new Path(uri);
        System.out.printf("### MAP ### caching path: %s (parent: %s)\n", path.toString(), path.getParent().toString());
        BufferedReader br = new BufferedReader(
            new InputStreamReader(
                new FileInputStream(path.getName())));
        String line;
        if (path.getParent().toString().endsWith(pathMovies)) {
          while ((line = br.readLine()) != null) {
            Map<String,String> movie = RecordParser.parse(line, new String[] {"id","title","genres"},",");
            long movieId = Long.valueOf(movie.get("id"));
            String movieTitle = movie.get("title");
            this.movieIdToMovieTitle.put(movieId, movieTitle);
            System.out.printf("### MAP ### cached movie title: (%d,%s)\n", movieId, movieTitle);
          }
        } else if (path.getParent().toString().endsWith(pathTopK)) {
          long movieTopKPosition = 1;
          while ((line = br.readLine()) != null) {
            Map<String,String> movie = RecordParser.parse(line, new String[] {"id","score"},",");
            long movieId = Long.valueOf(movie.get("id"));
            double movieTopKScore = Double.valueOf(movie.get("score"));
            this.movieIdToMovieTopKPositionAndScore.put(movieId, movieTopKPosition + ";" + movieTopKScore);
            System.out.printf("### MAP ### cached topk element : (%d,%d,%f)\n", movieTopKPosition, movieId, movieTopKScore);
            movieTopKPosition++;
          }
        }
        br.close();
      }
    } catch (IOException exc) {
      exc.printStackTrace();
    }
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
  public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
    Map<String,String> movie = RecordParser.parse(value.toString(), new String[] {"movieId","score"}, ",");

    long movieId = Long.valueOf(movie.get("movieId"));
    long rankPosition = key.get();
    double rankScore = Double.valueOf(movie.get("score"));

    System.out.printf("### MAP ### rank element: (%d,%d,%f)\n", rankPosition, movieId, rankScore);

    if (this.movieIdToMovieTopKPositionAndScore.containsKey(movieId)) {
      String rankDetails[] = this.movieIdToMovieTopKPositionAndScore.get(movieId).split(";");
      String movieTitle = this.movieIdToMovieTitle.get(movieId);
      long topKPosition = Long.valueOf(rankDetails[0]);
      double topKScore = Double.valueOf(rankDetails[1]);
      long delta = topKPosition - rankPosition;
      this.tuple.set(topKPosition + "," + movieId + "," + movieTitle + "," + topKScore + "," + rankPosition + "," + rankScore + "," + delta);
      ctx.write(NullWritable.get(), this.tuple);
      this.movieIdToMovieTopKPositionAndScore.remove(movieId);
      this.movieIdToMovieTitle.remove(movieId);
    }
  }

  /**
   * Flushes the mapper.
   *
   * @param ctx the job context.
   */
  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    if (!this.movieIdToMovieTopKPositionAndScore.isEmpty()) {
      for (Map.Entry<Long,String> entry : this.movieIdToMovieTopKPositionAndScore.entrySet()) {
        long movieId = entry.getKey();
        String rankDetails[] = entry.getValue().split(";");
        long topKPosition = Long.valueOf(rankDetails[0]);
        double topKScore = Double.valueOf(rankDetails[1]);
        this.tuple.set(topKPosition + "," + movieId + "," + topKScore + ",null,null,null");
        ctx.write(NullWritable.get(), this.tuple);
      }
    }
  }
}
