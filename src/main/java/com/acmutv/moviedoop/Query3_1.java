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

import com.acmutv.moviedoop.map.AverageRatingAsKeyMapper;
import com.acmutv.moviedoop.map.FilterRatingsByTimeIntervalMapper;
import com.acmutv.moviedoop.map.MoviesTopKBestMapMapper;
import com.acmutv.moviedoop.map.RankComparisonMapper;
import com.acmutv.moviedoop.reduce.AverageRatingReducer;
import com.acmutv.moviedoop.reduce.MoviesTopKBestMapReducer;
import com.acmutv.moviedoop.reduce.ValueReducer;
import com.acmutv.moviedoop.util.DateParser;
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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.time.LocalDateTime;

/**
 * A map/reduce program that returns the comparison between
 * (i) the top-`rankSize` movies, considering average ratings in period from `ratingTimestampTopKLB`
 * and `ratingTimestampTopKUB`; and
 * (ii) the total rank of moviues, considering average ratings in period from `ratingTimestampRankLB`
 * and `ratingTimestampRankUB`.
 * It leverages BestMap.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class Query3_1 extends Configured implements Tool {

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
  private static final boolean VERBOSE = false;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.printf("Usage: %s [-D prop=val] <in> <out>\n", PROGRAM_NAME);
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    // PATHS
    final Path input = new Path(args[0]);
    final Path stagingAverage = new Path(args[1] + "_stagingAverage");
    final Path stagingSort1 = new Path(args[1] + "_stagingSort1");
    final Path stagingSort2 = new Path(args[1] + "_stagingSort2");
    final Path sortPartitions = new Path(args[1] + "_partitions.sort.lst");
    final Path stagingTopK = new Path(args[1] + "_stagingTopK");
    final Path output = new Path(args[1]);

    // CONTEXT CONFIGURATION
    Configuration config = super.getConf();
    config.setIfUnset("movie.topk.size", String.valueOf(MOVIE_RANK_SIZE));
    config.setIfUnset("movie.rating.timestamp.topk.lb", DateParser.toString(MOVIE_RATINGS_TIMESTAMP_TOPK_LB));
    config.setIfUnset("movie.rating.timestamp.topk.ub", DateParser.toString(MOVIE_RATINGS_TIMESTAMP_TOPK_UB));
    config.setIfUnset("movie.rating.timestamp.sort.lb", DateParser.toString(MOVIE_RATINGS_TIMESTAMP_SORT_LB));
    config.setIfUnset("movie.rating.timestamp.sort.ub", DateParser.toString(MOVIE_RATINGS_TIMESTAMP_SORT_UB));


    // OTHER CONFIGURATION
    final int AVERAGE_REDUCE_CARDINALITY = Integer.valueOf(config.get("movie.average.reduce.cardinality", String.valueOf(MOVIE_AVERAGE_REDUCE_CARDINALITY)));
    final int TOPK_REDUCE_CARDINALITY = Integer.valueOf(config.get("movie.topk.reduce.cardinality", String.valueOf(MOVIE_TOPK_REDUCE_CARDINALITY)));
    final int SORT_REDUCE_CARDINALITY = Integer.valueOf(config.get("movie.sort.reduce.cardinality", String.valueOf(MOVIE_SORT_REDUCE_CARDINALITY)));
    final int SORT_PARTITION_SAMPLES = Integer.valueOf(config.get("movie.sort.partition.samples", String.valueOf(MOVIE_SORT_PARTITION_SAMPLES)));
    final double SORT_PARTITION_FREQUENCY = Double.valueOf(config.get("movie.sort.partition.frequency", String.valueOf(MOVIE_SORT_PARTITION_FREQUENCY)));
    final int SORT_PARTITION_SPLITS_MAX = Integer.valueOf(config.get("movie.sort.partition.splits.max", String.valueOf(MOVIE_SORT_PARTITION_SPLITS_MAX)));
    config.unset("movie.average.reduce.cardinality");
    config.unset("movie.topk.reduce.cardinality");
    config.unset("movie.sort.reduce.cardinality");
    config.unset("movie.sort.partition.samples");
    config.unset("movie.sort.partition.frequency");
    config.unset("movie.sort.partition.splits.max");

    // CONTEXT RESUME
    System.out.println("############################################################################");
    System.out.printf("%s\n", PROGRAM_NAME);
    System.out.println("****************************************************************************");
    System.out.println("Input: " + input);
    System.out.println("Output: " + output);
    System.out.println("Movie Top Rank Size: " + config.get("movie.topk.size"));
    System.out.println("Movie Rating Timestamp Lower Bound (Top Rank): " + config.get("movie.rating.timestamp.topk.lb"));
    System.out.println("Movie Rating Timestamp Upper Bound (Top Rank): " + config.get("movie.rating.timestamp.topk.ub"));
    System.out.println("Movie Rating Timestamp Lower Bound (Total Rank): " + config.get("movie.rating.timestamp.sort.lb"));
    System.out.println("Movie Rating Timestamp Upper Bound (Total Rank): " + config.get("movie.rating.timestamp.sort.ub"));
    System.out.println("----------------------------------------------------------------------------");
    System.out.println("Reduce Cardinality (Average): " + AVERAGE_REDUCE_CARDINALITY);
    System.out.println("Reduce Cardinality (Top Rank): " + TOPK_REDUCE_CARDINALITY);
    System.out.println("Reduce Cardinality (Total Rank): " + SORT_REDUCE_CARDINALITY);
    System.out.println("Movie Sorting Partition Samples: " + SORT_PARTITION_SAMPLES);
    System.out.println("Movie Sorting Partition Frequency: " + SORT_PARTITION_FREQUENCY);
    System.out.println("Movie Sorting Partition Max Splits: " + SORT_PARTITION_SPLITS_MAX);
    System.out.println("############################################################################");

    // JOB AVERAGE RATINGS: CONFIGURATION
    Job jobAverageRatings = Job.getInstance(config, PROGRAM_NAME + "_AVERAGE-RATINGS");
    jobAverageRatings.setJarByClass(Query3_1.class);

    // JOB AVERAGE RATINGS: MAP CONFIGURATION
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
    FileOutputFormat.setOutputPath(jobAverageRatings, stagingAverage);

    // JOB AVERAGE RATINGS: EXECUTION
    int code = jobAverageRatings.waitForCompletion(VERBOSE) ? 0 : 1;

    if (code == 0) {
      // JOB TOP BY RATING: CONFIGURATION
      Job jobTopRatings = Job.getInstance(config, PROGRAM_NAME + "_TOP-BY-RATING");
      jobTopRatings.setJarByClass(Query3_1.class);

      // JOB TOP BY RATING: MAP CONFIGURATION
      FileInputFormat.addInputPath(jobTopRatings, stagingAverage);
      jobTopRatings.setMapperClass(MoviesTopKBestMapMapper.class);
      jobTopRatings.setMapOutputKeyClass(NullWritable.class);
      jobTopRatings.setMapOutputValueClass(Text.class);

      // JOB TOP BY RATING: REDUCE CONFIGURATION
      jobTopRatings.setReducerClass(MoviesTopKBestMapReducer.class);
      jobTopRatings.setNumReduceTasks(TOPK_REDUCE_CARDINALITY);

      // JOB TOP BY RATING: OUTPUT CONFIGURATION
      jobTopRatings.setOutputKeyClass(NullWritable.class);
      jobTopRatings.setOutputValueClass(Text.class);
      FileOutputFormat.setOutputPath(jobTopRatings, stagingTopK);

      // JOB TOP BY RATING: JOB EXECUTION
      code = jobTopRatings.waitForCompletion(VERBOSE) ? 0 : 1;
    }

    if (code == 0) {
      // JOB RATING AS KEY: CONFIGURATION
      Job jobRatingAsKey = Job.getInstance(config, PROGRAM_NAME + "_RATING-AS-KEY");
      jobRatingAsKey.setJarByClass(QuerySort_1.class);

      // JOB RATING AS KEY: MAP CONFIGURATION
      FileInputFormat.addInputPath(jobRatingAsKey, stagingAverage);
      jobRatingAsKey.setMapperClass(AverageRatingAsKeyMapper.class);

      // JOB RATING AS KEY: REDUCE CONFIGURATION
      jobRatingAsKey.setNumReduceTasks(0);

      // JOB RATING AS KEY: OUTPUT CONFIGURATION
      jobRatingAsKey.setOutputKeyClass(Text.class);
      jobRatingAsKey.setOutputValueClass(Text.class);
      jobRatingAsKey.setOutputFormatClass(SequenceFileOutputFormat.class);
      SequenceFileOutputFormat.setOutputPath(jobRatingAsKey, stagingSort1);

      // JOB RATING AS KEY: JOB EXECUTION
      code = jobRatingAsKey.waitForCompletion(VERBOSE) ? 0 : 1;
    }

    if (code == 0) {
      // JOB SORT BY AVERAGE RATING: CONFIGURATION
      Job jobSortByRating = Job.getInstance(config, PROGRAM_NAME + "_SORT-BY-AVERAGE-RATING");
      jobSortByRating.setJarByClass(QuerySort_1.class);
      jobSortByRating.setSortComparatorClass(LongWritable.DecreasingComparator.class);

      // JOB SORT BY AVERAGE RATING: MAP CONFIGURATION
      jobSortByRating.setInputFormatClass(SequenceFileInputFormat.class);
      SequenceFileInputFormat.addInputPath(jobSortByRating, stagingSort1);
      jobSortByRating.setMapperClass(Mapper.class);

      // JOB SORT BY AVERAGE RATING: REDUCE CONFIGURATION
      jobSortByRating.setReducerClass(ValueReducer.class);
      jobSortByRating.setNumReduceTasks(SORT_REDUCE_CARDINALITY);

      // JOB SORT BY AVERAGE RATING: OUTPUT CONFIGURATION
      jobSortByRating.setOutputKeyClass(Text.class);
      jobSortByRating.setOutputValueClass(Text.class);
      FileOutputFormat.setOutputPath(jobSortByRating, stagingSort2);

      // JOB SORT BY AVERAGE RATING: PARTITIONER CONFIGURATION
      if (SORT_REDUCE_CARDINALITY > 1) {
        jobSortByRating.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(jobSortByRating.getConfiguration(), sortPartitions);
        jobSortByRating.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");
        InputSampler.RandomSampler<Text,Text> sampler = new InputSampler.RandomSampler<>(SORT_PARTITION_FREQUENCY, SORT_PARTITION_SAMPLES, SORT_PARTITION_SPLITS_MAX);
        InputSampler.writePartitionFile(jobSortByRating, sampler);
      }

      // JOB SORT BY AVERAGE RATING: EXECUTION
      code = jobSortByRating.waitForCompletion(VERBOSE) ? 0 : 1;
    }

    if (code == 0) {
      // JOB RANK COMPARISON: CONFIGURATION
      Job jobRankComparison = Job.getInstance(config, PROGRAM_NAME + "_RANK_COMPARISON");
      jobRankComparison.setJarByClass(Query3_1.class);
      for (FileStatus status : FileSystem.get(config).listStatus(stagingTopK)) {
        jobRankComparison.addCacheFile(status.getPath().toUri());
      }

      // JOB AVERAGE RATINGS: MAP CONFIGURATION
      FileInputFormat.addInputPath(jobRankComparison, input);
      jobRankComparison.setMapperClass(RankComparisonMapper.class);
      //jobRankComparison.setMapOutputKeyClass(LongWritable.class);
      //jobRankComparison.setMapOutputValueClass(DoubleWritable.class);

      // JOB AVERAGE RATINGS: REDUCE CONFIGURATION
      //jobRankComparison.setReducerClass(RankComparisonReducer.class);
      //jobRankComparison.setNumReduceTasks(1);

      // JOB AVERAGE RATINGS: OUTPUT CONFIGURATION
      jobRankComparison.setOutputKeyClass(NullWritable.class);
      jobRankComparison.setOutputValueClass(Text.class);
      FileOutputFormat.setOutputPath(jobRankComparison, output);

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
