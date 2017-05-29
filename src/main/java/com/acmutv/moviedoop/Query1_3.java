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
package com.acmutv.moviedoop;

import com.acmutv.moviedoop.map.FilterRatingsByTimestampJoinMovieTitleCachedMapper;
import com.acmutv.moviedoop.reduce.AverageRatingFilterReducer;
import com.acmutv.moviedoop.util.DateParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.time.LocalDateTime;

/**
 * A map/reduce program that returns movies with rate greater/equal to the specified {@code threshold}
 * and valuated starting from the specified {@code startDate}.
 * The program leverages inner joins (replication joins).
 * The program leverages distributed caching (on mapper).
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class Query1_3 extends Configured implements Tool {

  /**
   * The program name.
   */
  private static final String PROGRAM_NAME = "Query1_3";

  /**
   * The default lower bound for movie average rating.
   */
  private static final double MOVIE_RATING_AVERAGE_LB = 2.5;

  /**
   * The default lower bound for movie ratings timestamp.
   */
  private static final LocalDateTime MOVIE_RATINGS_TIMESTAMP_LB = DateParser.MIN;

  /**
   * The default number of reducers for the averaging job.
   */
  private static final int MOVIE_AVERAGE_REDUCE_CARDINALITY = 1;

  /**
   * The default verbosity.
   */
  private static final boolean VERBOSE = true;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.printf("Usage: %s [-D prop=val] <inRatings> <inMovies> <out>\n", PROGRAM_NAME);
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    // PATHS
    final Path inputRatings = new Path(args[0]);
    final Path inputMovies = new Path(args[1]);
    final Path output = new Path(args[2]);

    // CONTEXT CONFIGURATION
    Configuration config = super.getConf();
    config.setIfUnset("moviedoop.average.rating.lb", String.valueOf(MOVIE_RATING_AVERAGE_LB));
    config.setIfUnset("moviedoop.average.rating.timestamp.lb", DateParser.toString(MOVIE_RATINGS_TIMESTAMP_LB));

    // OTHER CONFIGURATION
    final int AVERAGE_REDUCE_CARDINALITY = Integer.valueOf(config.get("moviedoop.average.reduce.cardinality", String.valueOf(MOVIE_AVERAGE_REDUCE_CARDINALITY)));
    config.unset("moviedoop.average.reduce.cardinality");

    // CONFIGURATION RESUME
    System.out.println("############################################################################");
    System.out.printf("%s\n", PROGRAM_NAME);
    System.out.println("****************************************************************************");
    System.out.println("Input Ratings: " + inputRatings);
    System.out.println("Input Movies: " + inputMovies);
    System.out.println("Output: " + output);
    System.out.println("Movie Average Rating Lower Bound: " + config.get("moviedoop.average.rating.lb"));
    System.out.println("Movie Rating Timestamp Lower Bound: " + config.get("moviedoop.average.rating.timestamp.lb"));
    System.out.println("----------------------------------------------------------------------------");
    System.out.println("Reduce Cardinality (average): " + AVERAGE_REDUCE_CARDINALITY);
    System.out.println("############################################################################");

    // JOB CONFIGURATION
    Job job = Job.getInstance(config, PROGRAM_NAME);
    job.setJarByClass(Query1_3.class);
    FileSystem hdfs = FileSystem.get(job.getConfiguration());
    for (FileStatus status : hdfs.listStatus(inputMovies)) {
      job.addCacheFile(status.getPath().toUri());
    }

    // MAP CONFIGURATION
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, inputRatings);
    job.setMapperClass(FilterRatingsByTimestampJoinMovieTitleCachedMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    // REDUCE CONFIGURATION
    job.setReducerClass(AverageRatingFilterReducer.class);
    job.setNumReduceTasks(AVERAGE_REDUCE_CARDINALITY);

    // OUTPUT CONFIGURATION
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, output);

    // JOB EXECUTION
    return job.waitForCompletion(VERBOSE) ? 0 : 1;
  }

  /**
   * The program main method.
   *
   * @param args the program arguments.
   * @throws Exception when the program cannot be executed.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Query1_3(), args);
    System.exit(res);
  }
}
