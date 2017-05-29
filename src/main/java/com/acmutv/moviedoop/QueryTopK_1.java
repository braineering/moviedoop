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

import com.acmutv.moviedoop.map.*;
import com.acmutv.moviedoop.reduce.AverageRatingReducer;
import com.acmutv.moviedoop.reduce.MoviesTopKTreeMapReducer;
import com.acmutv.moviedoop.util.DateParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.time.LocalDateTime;

/**
 * A map/reduce program that returns the top-`rankSize` movies considering average ratings in
 * period from `ratingTimestampLB` and `ratingTimestampUB`.
 * It leverages TreeMap.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class QueryTopK_1 extends Configured implements Tool {

  /**
   * The program name.
   */
  private static final String PROGRAM_NAME = "QueryTopK_1";

  /**
   * The default movies rank size.
   */
  private static final int MOVIE_RANK_SIZE = 10;

  /**
   * The default lower bound for movie ratings timestamp.
   */
  private static final LocalDateTime MOVIE_RATINGS_TIMESTAMP_LB = DateParser.MIN;

  /**
   * The default upper bound for movie ratings timestamp.
   */
  private static final LocalDateTime MOVIE_RATINGS_TIMESTAMP_UB = DateParser.MAX;

  /**
   * The default number of reducers for the averaging job.
   */
  private static final int MOVIE_AVERAGE_REDUCE_CARDINALITY = 1;

  /**
   * The default number of reducers for the ranking job.
   */
  private static final int MOVIE_TOPK_REDUCE_CARDINALITY = 1;

  /**
   * The default verbosity.
   */
  private static final boolean VERBOSE = true;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.printf("Usage: %s [-D prop=val] <in> <out>\n", PROGRAM_NAME);
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    // PATHS
    final Path input = new Path(args[0]);
    final Path staging = new Path(args[1] + "_staging");
    final Path output = new Path(args[1]);

    // CONTEXT CONFIGURATION
    Configuration config = super.getConf();
    config.setIfUnset("moviedoop.topk.size", String.valueOf(MOVIE_RANK_SIZE));
    config.setIfUnset("moviedoop.average.rating.timestamp.lb", DateParser.toString(MOVIE_RATINGS_TIMESTAMP_LB));
    config.setIfUnset("moviedoop.average.rating.timestamp.ub", DateParser.toString(MOVIE_RATINGS_TIMESTAMP_UB));

    // OTHER CONFIGURATION
    final int AVERAGE_REDUCE_CARDINALITY = Integer.valueOf(config.get("moviedoop.average.reduce.cardinality", String.valueOf(MOVIE_AVERAGE_REDUCE_CARDINALITY)));
    final int TOPK_REDUCE_CARDINALITY = Integer.valueOf(config.get("moviedoop.topk.reduce.cardinality", String.valueOf(MOVIE_TOPK_REDUCE_CARDINALITY)));
    config.unset("moviedoop.average.reduce.cardinality");
    config.unset("moviedoop.topk.reduce.cardinality");

    // CONTEXT RESUME
    System.out.println("############################################################################");
    System.out.printf("%s\n", PROGRAM_NAME);
    System.out.println("****************************************************************************");
    System.out.println("Input: " + input);
    System.out.println("Output: " + output);
    System.out.println("Movie Top Rank Size: " + config.get("moviedoop.topk.size"));
    System.out.println("Movie Rating Timestamp Lower Bound (Top Ranking): " + config.get("moviedoop.average.rating.timestamp.lb"));
    System.out.println("Movie Rating Timestamp Upper Bound (Top Ranking): " + config.get("moviedoop.average.rating.timestamp.ub"));
    System.out.println("----------------------------------------------------------------------------");
    System.out.println("Reduce Cardinality (average): " + AVERAGE_REDUCE_CARDINALITY);
    System.out.println("Reduce Cardinality (topk): " + TOPK_REDUCE_CARDINALITY);
    System.out.println("############################################################################");

    // JOB AVERAGE RATINGS: CONFIGURATION
    Job jobAverageRatings = Job.getInstance(config, PROGRAM_NAME + "_AVERAGE-RATINGS");
    jobAverageRatings.setJarByClass(QueryTopK_1.class);

    // JOB AVERAGE RATINGS: MAP CONFIGURATION
    jobAverageRatings.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(jobAverageRatings, input);
    jobAverageRatings.setMapperClass(FilterRatingsByTimeIntervalMapper.class);
    jobAverageRatings.setMapOutputKeyClass(LongWritable.class);
    jobAverageRatings.setMapOutputValueClass(DoubleWritable.class);

    // JOB AVERAGE RATINGS: REDUCE CONFIGURATION
    jobAverageRatings.setReducerClass(AverageRatingReducer.class);
    jobAverageRatings.setNumReduceTasks(AVERAGE_REDUCE_CARDINALITY);

    // JOB AVERAGE RATINGS: OUTPUT CONFIGURATION
    jobAverageRatings.setOutputKeyClass(NullWritable.class);
    jobAverageRatings.setOutputValueClass(Text.class);
    jobAverageRatings.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(jobAverageRatings, staging);

    // JOB AVERAGE RATINGS: EXECUTION
    int code = jobAverageRatings.waitForCompletion(VERBOSE) ? 0 : 1;

    if (code == 0) {
      // JOB TOP BY RATING: CONFIGURATION
      Job jobTopRatings = Job.getInstance(config, PROGRAM_NAME + "_TOP-BY-RATING");
      jobTopRatings.setJarByClass(QueryTopK_1.class);

      // JOB TOP BY RATING: MAP CONFIGURATION
      jobTopRatings.setInputFormatClass(SequenceFileInputFormat.class);
      SequenceFileInputFormat.addInputPath(jobTopRatings, staging);
      jobTopRatings.setMapperClass(MoviesTopKTreeMapMapper.class);
      jobTopRatings.setMapOutputKeyClass(NullWritable.class);
      jobTopRatings.setMapOutputValueClass(Text.class);

      // JOB TOP BY RATING: REDUCE CONFIGURATION
      jobTopRatings.setReducerClass(MoviesTopKTreeMapReducer.class);
      jobTopRatings.setNumReduceTasks(TOPK_REDUCE_CARDINALITY);

      // JOB TOP BY RATING: OUTPUT CONFIGURATION
      jobTopRatings.setOutputKeyClass(NullWritable.class);
      jobTopRatings.setOutputValueClass(Text.class);
      jobTopRatings.setOutputFormatClass(TextOutputFormat.class);
      FileOutputFormat.setOutputPath(jobTopRatings, output);

      // JOB TOP BY RATING: JOB EXECUTION
      code = jobTopRatings.waitForCompletion(VERBOSE) ? 0 : 1;
    }

    // CLEAN STAGING OUTPUT
    FileSystem.get(config).delete(staging, true);

    return code;
  }

  /**
   * The program main method.
   *
   * @param args the program arguments.
   * @throws Exception when the program cannot be executed.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new QueryTopK_1(), args);
    System.exit(res);
  }
}
