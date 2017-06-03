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

import com.acmutv.moviedoop.common.util.RecordParser;
import com.acmutv.moviedoop.query3.Query3_4;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * The mapper for jobs in: {@link Query3_4}.
 * It emits (movieId,rating) where rating is a score attributed with timestamp greater or equal to
 * the `movieRatingTimestampLowerBound`.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class RankComparisonMapperMixed extends Mapper<LongWritable,Text,NullWritable,Text> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(RankComparisonMapperMixed.class);

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
    LOG.debug("[SETUP] moviedoop.path.movies: " + pathMovies);
    LOG.debug("[SETUP] moviedoop.path.topk: " + pathTopK);

    try {
      for (URI uri : ctx.getCacheFiles()) {
        Path path = new Path(uri);
        if (path.getParent().toString().endsWith(pathMovies)) {
          System.out.printf("### MAP ### reading cached file (movies): %s\n", path);
          Reader reader = OrcFile.createReader(path, new OrcFile.ReaderOptions(ctx.getConfiguration()));
          RecordReader rows = reader.rows();
          VectorizedRowBatch batch = reader.getSchema().createRowBatch();
          while (rows.nextBatch(batch)) {
            BytesColumnVector cvMovieId = (BytesColumnVector) batch.cols[0];
            BytesColumnVector cvMovieTitle = (BytesColumnVector) batch.cols[1];
            for (int r = 0; r < batch.size; r++) {
              long movieId = Long.valueOf(cvMovieId.toString(r));
              String movieTitle = cvMovieTitle.toString(r);
              this.movieIdToMovieTitle.put(movieId, movieTitle);
            }
          }
          rows.close();
        } else if (path.getParent().toString().endsWith(pathTopK) && !"_SUCCESS".equals(path.getName())) {
          System.out.printf("### MAP ### reading cached file (topk): %s\n", path);
          Reader reader = OrcFile.createReader(path, new OrcFile.ReaderOptions(ctx.getConfiguration()));
          RecordReader rows = reader.rows();
          VectorizedRowBatch batch = reader.getSchema().createRowBatch();
          while (rows.nextBatch(batch)) {
            LongColumnVector cvMovieId = (LongColumnVector) batch.cols[0];
            DoubleColumnVector cvMovieScore = (DoubleColumnVector) batch.cols[1];
            for (int r = 0; r < batch.size; r++) {
              long movieId = cvMovieId.vector[r];
              double movieTopKScore = cvMovieScore.vector[r];
              long movieTopKPosition = r + 1;
              this.movieIdToMovieTopKPositionAndScore.put(movieId, movieTopKPosition + ";" + movieTopKScore);
            }
          }
          rows.close();
        }
      }
    } catch (IOException exc) {
      LOG.error(exc.getMessage());
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

    if (this.movieIdToMovieTopKPositionAndScore.containsKey(movieId)) {
      String topkDetails[] = this.movieIdToMovieTopKPositionAndScore.get(movieId).split(";");
      String movieTitle = this.movieIdToMovieTitle.get(movieId);
      long topKPosition = Long.valueOf(topkDetails[0]);
      double topkScore = Double.valueOf(topkDetails[1]);
      long deltaPosition = rankPosition - topKPosition;
      double deltaScore = topkScore - rankScore;
      this.tuple.set(movieTitle + "\t" + deltaPosition + "\t" + deltaScore);
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
        String movieTitle = this.movieIdToMovieTitle.get(movieId);
        this.tuple.set(movieTitle + "\tna\tna");
        ctx.write(NullWritable.get(), this.tuple);
      }
    }
  }
}
