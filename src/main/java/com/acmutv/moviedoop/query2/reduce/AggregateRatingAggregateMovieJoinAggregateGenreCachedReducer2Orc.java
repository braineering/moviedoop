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

import com.acmutv.moviedoop.query1.Query1_1;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

import java.io.IOException;
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
public class AggregateRatingAggregateMovieJoinAggregateGenreCachedReducer2Orc extends Reducer<OrcKey,OrcValue,NullWritable,OrcStruct> {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(AggregateRatingAggregateMovieJoinAggregateGenreCachedReducer2Orc.class);

  /**
   * The null writable value.
   */
  private static final NullWritable NULL = NullWritable.get();

  /**
   * The ORC schema.
   */
  public static final TypeDescription ORC_SCHEMA = TypeDescription.fromString("struct<genre:string,ratings:string>");

  /**
   * The ORC struct for value.
   */
  private OrcStruct valueStruct = (OrcStruct) OrcStruct.createValue(ORC_SCHEMA);

  /**
   * The genre name to emit.
   */
  private Text genreTitle = (Text) valueStruct.getFieldValue(0);

  /**
   * The tuple {rating=repetitions,...,rating=repetitions} to emit.
   */
  private Text ratings = (Text) valueStruct.getFieldValue(1);

  /**
   * The cached map (movieId,movieTitle)
   */
  private Map<Long,String> movieIdToGenres = new HashMap<>();

  /**
   *
   */
  private Map<Double,Long> allRatingsForAMovie = new HashMap<>();

  /**
   * Configures the reducer.
   *
   * @param ctx the job context.
   */
  protected void setup(Context ctx) {
    try {
      for (URI uri : ctx.getCacheFiles()) {
        Path path = new Path(uri);
        Reader reader = OrcFile.createReader(path, new OrcFile.ReaderOptions(ctx.getConfiguration()));
        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        while (rows.nextBatch(batch)) {
          BytesColumnVector cvMovieId = (BytesColumnVector) batch.cols[0];
          BytesColumnVector cvGeneres = (BytesColumnVector) batch.cols[2];
          for (int r = 0; r < batch.size; r++) {
            long movieId = Long.valueOf(cvMovieId.toString(r));
            String genres = cvGeneres.toString(r);
            if(!genres.equals("(no genres listed)"))
              this.movieIdToGenres.put(movieId,genres);
          }
        }
        rows.close();
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
  public void reduce(OrcKey key, Iterable<OrcValue> values, Context ctx) throws IOException, InterruptedException {
    long movieId = ((LongWritable) ((OrcStruct) key.key).getFieldValue(0)).get();

    this.allRatingsForAMovie.clear();

    for (OrcValue orcValue : values) {
      String value = ((Text) ((OrcStruct) orcValue.value).getFieldValue(0)).toString();
      String pairs[] = value.split(",", -1);
      for (String pair : pairs) {
        String elems[] = pair.split("=", -1);
        double score = Double.valueOf(elems[0]);
        long repetitions = Long.valueOf(elems[1]);
        this.allRatingsForAMovie.put(score, repetitions);
      }
    }

    if (this.movieIdToGenres.containsKey(movieId)) {
      String[] genres = this.movieIdToGenres.get(movieId).split("\\|");
      for (String genre : genres) {
        this.genreTitle.set(genre);
        String report = this.allRatingsForAMovie.toString().replaceAll(" ", "");
        report = report.substring(1, report.length() - 1);
        this.ratings.set(report);
        ctx.write(NULL, this.valueStruct);
      }
    }
  }
}
