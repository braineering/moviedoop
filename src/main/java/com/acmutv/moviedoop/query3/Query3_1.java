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
package com.acmutv.moviedoop.query3;

import com.acmutv.moviedoop.common.input.LinenoSequenceFileInputFormat;
import com.acmutv.moviedoop.query3.map.*;
import com.acmutv.moviedoop.query3.reduce.AverageRating2Reducer;
import com.acmutv.moviedoop.query3.reduce.MoviesTopKBestMapReducer;
import com.acmutv.moviedoop.query3.reduce.ValueReducer;
import com.acmutv.moviedoop.test.QuerySort_1;
import com.acmutv.moviedoop.common.util.DateParser;
import com.acmutv.moviedoop.common.util.DoubleWritableDecreasingComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.time.LocalDateTime;

/**
 * A map/reduce program that returns the comparison between
 * (i) the top-`rankSize` movies, considering average ratings in period from `ratingTimestampTopKLB`
 * and `ratingTimestampTopKUB`; and
 * (ii) the total rank of movies, considering average ratings in period from `ratingTimestampRankLB`
 * and `ratingTimestampRankUB`.
 * The program leverages BestMap for top-k ranking and inner joins (replication joins as distributed
 * caching on map).
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class Query3_1 extends Configured implements Tool {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(Query3_1.class);

  /**
   * The program name.
   */
  private static final String PROGRAM_NAME = "Query3_1";

  /**
   * The default movies rank size.
   */
  private static final int MOVIE_RANK_SIZE = 10;

  /**
   * The default lower bound for movie ratings timestamp (for top-k).
   */
  private static final LocalDateTime MOVIE_RATINGS_TIMESTAMP_TOPK_LB = DateParser.MIN;

  /**
   * The default upper bound for movie ratings timestamp (for top-k).
   */
  private static final LocalDateTime MOVIE_RATINGS_TIMESTAMP_TOPK_UB = DateParser.MAX;

  /**
   * The default lower bound for movie ratings timestamp (for total rank).
   */
  private static final LocalDateTime MOVIE_RATINGS_TIMESTAMP_SORT_LB = DateParser.MIN;

  /**
   * The default upper bound for movie ratings timestamp (for total rank).
   */
  private static final LocalDateTime MOVIE_RATINGS_TIMESTAMP_SORT_UB = DateParser.MAX;

  /**
   * The default number of reducers for the averaging job.
   */
  private static final int MOVIE_AVERAGE_REDUCE_CARDINALITY = 1;

  /**
   * The default number of reducers for the ranking job (top-k).
   */
  private static final int MOVIE_TOPK_REDUCE_CARDINALITY = 1;

  /**
   * The default number of reducers for the ranking job (total rank).
   */
  private static final int MOVIE_SORT_REDUCE_CARDINALITY = 1;

  /**
   * The default number of sorting partitioner samples.
   */
  private static final int MOVIE_SORT_PARTITION_SAMPLES = 1000;

  /**
   * The default frequency for sorting partitioner (total rank).
   */
  private static final double MOVIE_SORT_PARTITION_FREQUENCY = 0.01;

  /**
   * The default maximum number of splits for sorting partition (total rank).
   */
  private static final int MOVIE_SORT_PARTITION_SPLITS_MAX = 100;

  /**
   * The default verbosity.
   */
  private static final boolean VERBOSE = true;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.printf("Usage: %s [-D prop=val] <inRatings> <inMovies> <out>\n", PROGRAM_NAME);
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    // PATHS
    final Path inputRatings = new Path(args[0]);
    final Path inputMovies = new Path(args[1]);
    final Path stagingAverage = new Path(args[2] + ".staging/average");
    final Path stagingSort1 = new Path(args[2] + ".staging/sort/1");
    final Path stagingSort2 = new Path(args[2] + ".staging/sort/2");
    final Path sortPartitions = new Path(args[2] + ".partitions.sort.lst");
    final Path stagingTopK = new Path(args[2] + ".staging/topk");
    final Path output = new Path(args[2]);

    // CONTEXT CONFIGURATION
    Configuration config = super.getConf();
    config.setIfUnset("moviedoop.topk.size", String.valueOf(MOVIE_RANK_SIZE));
    config.setIfUnset("moviedoop.average.rating.timestamp.lb.1", DateParser.toString(MOVIE_RATINGS_TIMESTAMP_TOPK_LB));
    config.setIfUnset("moviedoop.average.rating.timestamp.ub.1", DateParser.toString(MOVIE_RATINGS_TIMESTAMP_TOPK_UB));
    config.setIfUnset("moviedoop.average.rating.timestamp.lb.2", DateParser.toString(MOVIE_RATINGS_TIMESTAMP_SORT_LB));
    config.setIfUnset("moviedoop.average.rating.timestamp.ub.2", DateParser.toString(MOVIE_RATINGS_TIMESTAMP_SORT_UB));

    // OTHER CONFIGURATION
    final int AVERAGE_REDUCE_CARDINALITY = Integer.valueOf(config.get("moviedoop.average.reduce.cardinality", String.valueOf(MOVIE_AVERAGE_REDUCE_CARDINALITY)));
    final int TOPK_REDUCE_CARDINALITY = Integer.valueOf(config.get("moviedoop.topk.reduce.cardinality", String.valueOf(MOVIE_TOPK_REDUCE_CARDINALITY)));
    final int SORT_REDUCE_CARDINALITY = Integer.valueOf(config.get("moviedoop.sort.reduce.cardinality", String.valueOf(MOVIE_SORT_REDUCE_CARDINALITY)));
    final int SORT_PARTITION_SAMPLES = Integer.valueOf(config.get("moviedoop.sort.partition.samples", String.valueOf(MOVIE_SORT_PARTITION_SAMPLES)));
    final double SORT_PARTITION_FREQUENCY = Double.valueOf(config.get("moviedoop.sort.partition.frequency", String.valueOf(MOVIE_SORT_PARTITION_FREQUENCY)));
    final int SORT_PARTITION_SPLITS_MAX = Integer.valueOf(config.get("moviedoop.sort.partition.splits.max", String.valueOf(MOVIE_SORT_PARTITION_SPLITS_MAX)));
    config.unset("moviedoop.average.reduce.cardinality");
    config.unset("moviedoop.topk.reduce.cardinality");
    config.unset("moviedoop.sort.reduce.cardinality");
    config.unset("moviedoop.sort.partition.samples");
    config.unset("moviedoop.sort.partition.frequency");
    config.unset("moviedoop.sort.partition.splits.max");

    // CONTEXT RESUME
    System.out.println("############################################################################");
    System.out.printf("%s\n", PROGRAM_NAME);
    System.out.println("****************************************************************************");
    System.out.println("Input Ratings: " + inputRatings);
    System.out.println("Input Movies: " + inputMovies);
    System.out.println("Output: " + output);
    System.out.println("Movie Top Rank Size: " + config.get("moviedoop.topk.size"));
    System.out.println("Movie Rating Timestamp Lower Bound (Top Rank): " + config.get("moviedoop.average.rating.timestamp.lb.1"));
    System.out.println("Movie Rating Timestamp Upper Bound (Top Rank): " + config.get("moviedoop.average.rating.timestamp.ub.1"));
    System.out.println("Movie Rating Timestamp Lower Bound (Total Rank): " + config.get("moviedoop.average.rating.timestamp.lb.2"));
    System.out.println("Movie Rating Timestamp Upper Bound (Total Rank): " + config.get("moviedoop.average.rating.timestamp.ub.2"));
    System.out.println("----------------------------------------------------------------------------");
    System.out.println("Reduce Cardinality (Average): " + AVERAGE_REDUCE_CARDINALITY);
    System.out.println("Reduce Cardinality (Top Rank): " + TOPK_REDUCE_CARDINALITY);
    System.out.println("Reduce Cardinality (Total Rank): " + SORT_REDUCE_CARDINALITY);
    System.out.println("Movie Sorting Partition Samples: " + SORT_PARTITION_SAMPLES);
    System.out.println("Movie Sorting Partition Frequency: " + SORT_PARTITION_FREQUENCY);
    System.out.println("Movie Sorting Partition Max Splits: " + SORT_PARTITION_SPLITS_MAX);
    System.out.println("############################################################################");

    /* *********************************************************************************************
    * AVERAGE MOVIE RATINGS FOR PERIODS [Tlb1,Tub1] and [Tlb2,Tub2]
    ***********************************************************************************************/

    // JOB AVERAGE RATINGS: CONFIGURATION
    Job jobAverageRatings = Job.getInstance(config, PROGRAM_NAME + "_AVERAGE-RATINGS");
    jobAverageRatings.setJarByClass(Query3_1.class);

    // JOB AVERAGE RATINGS: MAP CONFIGURATION
    jobAverageRatings.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(jobAverageRatings, inputRatings);
    jobAverageRatings.setMapperClass(FilterRatingsBy2TimeIntervalMapper.class);
    jobAverageRatings.setMapOutputKeyClass(LongWritable.class);
    jobAverageRatings.setMapOutputValueClass(Text.class);

    // JOB AVERAGE RATINGS: REDUCE CONFIGURATION
    jobAverageRatings.setReducerClass(AverageRating2Reducer.class);
    jobAverageRatings.setNumReduceTasks(AVERAGE_REDUCE_CARDINALITY);

    // JOB AVERAGE RATINGS: OUTPUT CONFIGURATION
    MultipleOutputs.addNamedOutput(jobAverageRatings, "1", SequenceFileOutputFormat.class, NullWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(jobAverageRatings, "2", SequenceFileOutputFormat.class, NullWritable.class, Text.class);
    jobAverageRatings.setOutputFormatClass(LazyOutputFormat.class);
    LazyOutputFormat.setOutputFormatClass(jobAverageRatings, SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(jobAverageRatings, stagingAverage);

    // JOB AVERAGE RATINGS: EXECUTION
    int code = jobAverageRatings.waitForCompletion(VERBOSE) ? 0 : 1;

    /* *********************************************************************************************
     * TOP-K RANK OF MOVIES BY AVERAGE MOVIE RATINGS IN PERIOD [Tlb1,Tub1]
     **********************************************************************************************/

    if (code == 0) {
      // JOB TOP BY RATING: CONFIGURATION
      Job jobTopRatings = Job.getInstance(config, PROGRAM_NAME + "_TOP-BY-RATING");
      jobTopRatings.setJarByClass(Query3_1.class);

      // JOB TOP BY RATING: MAP CONFIGURATION
      jobTopRatings.setInputFormatClass(SequenceFileInputFormat.class);
      for (FileStatus status : FileSystem.get(config).listStatus(stagingAverage)) {
        Path path = status.getPath();
        if (path.getName().startsWith("1-r")) {
          SequenceFileInputFormat.addInputPath(jobTopRatings, path);
        }
      }

      jobTopRatings.setMapperClass(MoviesTopKBestMapMapper.class);
      jobTopRatings.setMapOutputKeyClass(NullWritable.class);
      jobTopRatings.setMapOutputValueClass(Text.class);

      // JOB TOP BY RATING: REDUCE CONFIGURATION
      jobTopRatings.setReducerClass(MoviesTopKBestMapReducer.class);
      jobTopRatings.setNumReduceTasks(TOPK_REDUCE_CARDINALITY);

      // JOB TOP BY RATING: OUTPUT CONFIGURATION
      jobTopRatings.setOutputKeyClass(NullWritable.class);
      jobTopRatings.setOutputValueClass(Text.class);
      jobTopRatings.setOutputFormatClass(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(jobTopRatings, stagingTopK);

      // JOB TOP BY RATING: JOB EXECUTION
      code = jobTopRatings.waitForCompletion(VERBOSE) ? 0 : 1;
    }

    /* *********************************************************************************************
     * TOTAL RANK OF MOVIES BY AVERAGE MOVIE RATINGS IN PERIOD [Tlb2,Tub2]
     **********************************************************************************************/
    if (code == 0) {
      // JOB RATING AS KEY: CONFIGURATION
      Job jobRatingAsKey = Job.getInstance(config, PROGRAM_NAME + "_RATING-AS-KEY");
      jobRatingAsKey.setJarByClass(Query3_1.class);

      // JOB RATING AS KEY: MAP CONFIGURATION
      jobRatingAsKey.setInputFormatClass(SequenceFileInputFormat.class);
      for (FileStatus status : FileSystem.get(config).listStatus(stagingAverage)) {
        Path path = status.getPath();
        if (path.getName().startsWith("2-r")) {
          SequenceFileInputFormat.addInputPath(jobRatingAsKey, path);
        }
      }
      jobRatingAsKey.setMapperClass(AverageRatingAsKeyMapper.class);

      // JOB RATING AS KEY: REDUCE CONFIGURATION
      jobRatingAsKey.setNumReduceTasks(0);

      // JOB RATING AS KEY: OUTPUT CONFIGURATION
      jobRatingAsKey.setOutputKeyClass(DoubleWritable.class);
      jobRatingAsKey.setOutputValueClass(Text.class);
      jobRatingAsKey.setOutputFormatClass(SequenceFileOutputFormat.class);
      SequenceFileOutputFormat.setOutputPath(jobRatingAsKey, stagingSort1);

      // JOB RATING AS KEY: JOB EXECUTION
      code = jobRatingAsKey.waitForCompletion(VERBOSE) ? 0 : 1;
    }

    if (code == 0) {
      // JOB SORT BY AVERAGE RATING: CONFIGURATION
      Job jobSortByRating = Job.getInstance(config, PROGRAM_NAME + "_SORT-BY-AVERAGE-RATING");
      jobSortByRating.setJarByClass(Query3_1.class);
      jobSortByRating.setSortComparatorClass(DoubleWritableDecreasingComparator.class);

      // JOB SORT BY AVERAGE RATING: MAP CONFIGURATION
      jobSortByRating.setInputFormatClass(SequenceFileInputFormat.class);
      SequenceFileInputFormat.addInputPath(jobSortByRating, stagingSort1);
      jobSortByRating.setMapperClass(IdentityMapper2.class);
      jobSortByRating.setMapOutputKeyClass(DoubleWritable.class);
      jobSortByRating.setMapOutputValueClass(Text.class);

      // JOB SORT BY AVERAGE RATING: REDUCE CONFIGURATION
      jobSortByRating.setReducerClass(ValueReducer.class);
      jobSortByRating.setNumReduceTasks(SORT_REDUCE_CARDINALITY);

      // JOB SORT BY AVERAGE RATING: OUTPUT CONFIGURATION
      jobSortByRating.setOutputKeyClass(NullWritable.class);
      jobSortByRating.setOutputValueClass(Text.class);
      jobSortByRating.setOutputFormatClass(SequenceFileOutputFormat.class);
      SequenceFileOutputFormat.setOutputPath(jobSortByRating, stagingSort2);

      // JOB SORT BY AVERAGE RATING: PARTITIONER CONFIGURATION
      if (SORT_REDUCE_CARDINALITY > 1) {
        jobSortByRating.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(jobSortByRating.getConfiguration(), sortPartitions);
        jobSortByRating.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");
        InputSampler.RandomSampler<DoubleWritable,Text> sampler = new InputSampler.RandomSampler<>(SORT_PARTITION_FREQUENCY, SORT_PARTITION_SAMPLES, SORT_PARTITION_SPLITS_MAX);
        InputSampler.writePartitionFile(jobSortByRating, sampler);
      }

      // JOB SORT BY AVERAGE RATING: EXECUTION
      code = jobSortByRating.waitForCompletion(VERBOSE) ? 0 : 1;
    }

    /* *********************************************************************************************
     * RANK COMPARISON
     **********************************************************************************************/
    if (code == 0) {
      // JOB RANK COMPARISON: CONFIGURATION
      Job jobRankComparison = Job.getInstance(config, PROGRAM_NAME + "_RANK_COMPARISON");
      jobRankComparison.setJarByClass(Query3_1.class);
      for (FileStatus status : FileSystem.get(config).listStatus(stagingTopK)) {
        Path path = status.getPath();
        if ("_SUCCESS".equals(path.getName())) continue;
        jobRankComparison.addCacheFile(path.toUri());
      }
      for (FileStatus status : FileSystem.get(config).listStatus(inputMovies)) {
        jobRankComparison.addCacheFile(status.getPath().toUri());
      }
      jobRankComparison.getConfiguration().setIfUnset("moviedoop.path.movies", inputMovies.toString());
      jobRankComparison.getConfiguration().setIfUnset("moviedoop.path.topk", stagingTopK.toString());

      // JOB AVERAGE RATINGS: MAP CONFIGURATION
      jobRankComparison.setInputFormatClass(LinenoSequenceFileInputFormat.class);
      LinenoSequenceFileInputFormat.addInputPath(jobRankComparison, stagingSort2);
      jobRankComparison.setMapperClass(RankComparisonMapper.class);

      // JOB AVERAGE RATINGS: REDUCE CONFIGURATION
      jobRankComparison.setNumReduceTasks(0);

      // JOB AVERAGE RATINGS: OUTPUT CONFIGURATION
      jobRankComparison.setOutputKeyClass(NullWritable.class);
      jobRankComparison.setOutputValueClass(Text.class);
      jobRankComparison.setOutputFormatClass(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(jobRankComparison, output);

      // JOB AVERAGE RATINGS: EXECUTION
      code = jobRankComparison.waitForCompletion(VERBOSE) ? 0 : 1;
    }

    // CLEAN STAGING OUTPUT
    FileSystem.get(config).delete(stagingAverage, true);
    FileSystem.get(config).delete(stagingTopK, true);
    FileSystem.get(config).delete(stagingSort1, true);
    FileSystem.get(config).delete(stagingSort2, true);
    FileSystem.get(config).delete(sortPartitions, true);

    return code;
  }

  /**
   * The program main method.
   *
   * @param args the program arguments.
   * @throws Exception when the program cannot be executed.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Query3_1(), args);
    System.exit(res);
  }
}
