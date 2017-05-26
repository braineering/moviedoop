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
import com.acmutv.moviedoop.reduce.AverageRatingReducer;
import com.acmutv.moviedoop.reduce.ValueReducer;
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
 * A map/reduce program that returns the total sorting of movies considering average ratings in
 * period from `ratingTimestampLB` and `ratingTimestampUB`.
 * It leverages the total sorting pattern.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class QuerySort_1 extends Configured implements Tool {

  /**
   * The program name.
   */
  private static final String PROGRAM_NAME = "QuerySort_1";

  /**
   * The default lower bound for movie ratings timestamp.
   */
  private static final LocalDateTime MOVIE_RATINGS_TIMESTAMP_LB = DateParser.MIN;

  /**
   * The default upper bound for movie ratings timestamp.
   */
  private static final LocalDateTime MOVIE_RATINGS_TIMESTAMP_UB = DateParser.MAX;

  /**
   * The default number of sorting reducers.
   */
  private static final int MOVIE_SORTING_REDUCE_CARDINALITY = 1;

  /**
   * The default number of sorting partitioner samples.
   */
  private static final int MOVIE_SORTING_PARTITION_SAMPLES = 1000;

  /**
   * The default frequency for sorting partitioner.
   */
  private static final double MOVIE_SORTING_PARTITION_FREQUENCY = 0.01;

  /**
   * The default maximum number of splits for sorting partition.
   */
  private static final int MOVIE_SORTING_PARTITION_SPLITS_MAX = 100;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.printf("Usage: %s [-D prop=val] <in> <out>\n", PROGRAM_NAME);
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    // PATHS
    final Path input = new Path(args[0]);
    final Path parts = new Path(args[1] + "_partitions.lst");
    final Path staging1 = new Path(args[1] + "_staging1");
    final Path staging2 = new Path(args[1] + "_staging2");
    final Path output = new Path(args[1]);

    // CONTEXT CONFIGURATION
    Configuration config = super.getConf();
    config.setIfUnset("movie.rating.timestamp.lb", DateParser.toString(MOVIE_RATINGS_TIMESTAMP_LB));
    config.setIfUnset("movie.rating.timestamp.ub", DateParser.toString(MOVIE_RATINGS_TIMESTAMP_UB));

    // OTHER CONFIGURATION
    final int SORTING_REDUCE_CARDINALITY = Integer.valueOf(config.get("movie.sorting.reduce.cardinality", String.valueOf(MOVIE_SORTING_REDUCE_CARDINALITY)));
    final int SORTING_PARTITION_SAMPLES = Integer.valueOf(config.get("movie.sorting.partition.samples", String.valueOf(MOVIE_SORTING_PARTITION_SAMPLES)));
    final double SORTING_PARTITION_FREQUENCY = Double.valueOf(config.get("movie.sorting.partition.frequency", String.valueOf(MOVIE_SORTING_PARTITION_FREQUENCY)));
    final int SORTING_PARTITION_SPLITS_MAX = Integer.valueOf(config.get("movie.sorting.partition.splits.max", String.valueOf(MOVIE_SORTING_PARTITION_SPLITS_MAX)));
    config.unset("movie.sorting.reduce.cardinality");
    config.unset("movie.sorting.partition.samples");
    config.unset("movie.sorting.partition.frequency");
    config.unset("movie.sorting.partition.splits.max");

    // CONTEXT RESUME
    System.out.println("############################################################################");
    System.out.printf("%s\n", PROGRAM_NAME);
    System.out.println("****************************************************************************");
    System.out.println("Input: " + input);
    System.out.println("Output: " + output);
    System.out.println("Movie Rating Timestamp Lower Bound (Total Ranking): " + config.get("movie.rating.timestamp.lb"));
    System.out.println("Movie Rating Timestamp Upper Bound (Total Ranking): " + config.get("movie.rating.timestamp.ub"));
    System.out.println("----------------------------------------------------------------------------");
    System.out.println("Movie Sorting Reduce Cardinality: " + SORTING_REDUCE_CARDINALITY);
    System.out.println("Movie Sorting Partition Samples: " + SORTING_PARTITION_SAMPLES);
    System.out.println("Movie Sorting Partition Frequency: " + SORTING_PARTITION_FREQUENCY);
    System.out.println("Movie Sorting Partition Max Splits: " + SORTING_PARTITION_SPLITS_MAX);
    System.out.println("############################################################################");

    // JOB AVERAGE RATINGS: CONFIGURATION
    Job jobAverageRatings = Job.getInstance(config, PROGRAM_NAME + "_AVERAGE-RATINGS");
    jobAverageRatings.setJarByClass(QuerySort_1.class);

    // JOB AVERAGE RATINGS: MAP CONFIGURATION
    FileInputFormat.addInputPath(jobAverageRatings, input);
    jobAverageRatings.setMapperClass(FilterRatingsByTimeIntervalMapper.class);
    jobAverageRatings.setMapOutputKeyClass(LongWritable.class);
    jobAverageRatings.setMapOutputValueClass(DoubleWritable.class);

    // JOB AVERAGE RATINGS: REDUCE CONFIGURATION
    jobAverageRatings.setReducerClass(AverageRatingReducer.class);
    jobAverageRatings.setNumReduceTasks(1);

    // JOB AVERAGE RATINGS: OUTPUT CONFIGURATION
    jobAverageRatings.setOutputKeyClass(NullWritable.class);
    jobAverageRatings.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(jobAverageRatings, staging1);

    // JOB AVERAGE RATINGS: EXECUTION
    int code = jobAverageRatings.waitForCompletion(true) ? 0 : 1;

    if (code == 0) {
      // JOB RATING AS KEY: CONFIGURATION
      Job jobRatingAsKey = Job.getInstance(config, PROGRAM_NAME + "_RATING-AS-KEY");
      jobRatingAsKey.setJarByClass(QuerySort_1.class);

      // JOB RATING AS KEY: MAP CONFIGURATION
      FileInputFormat.addInputPath(jobRatingAsKey, staging1);
      jobRatingAsKey.setMapperClass(AverageRatingAsKeyMapper.class);

      // JOB RATING AS KEY: REDUCE CONFIGURATION
      jobRatingAsKey.setNumReduceTasks(0);

      // JOB RATING AS KEY: OUTPUT CONFIGURATION
      jobRatingAsKey.setOutputKeyClass(Text.class);
      jobRatingAsKey.setOutputValueClass(Text.class);
      jobRatingAsKey.setOutputFormatClass(SequenceFileOutputFormat.class);
      SequenceFileOutputFormat.setOutputPath(jobRatingAsKey, staging2);

      // JOB RATING AS KEY: JOB EXECUTION
      code = jobRatingAsKey.waitForCompletion(true) ? 0 : 1;
    }

    if (code == 0) {
      // JOB SORT BY AVERAGE RATING: CONFIGURATION
      Job jobSortByRating = Job.getInstance(config, PROGRAM_NAME + "_SORT-BY-AVERAGE-RATING");
      jobSortByRating.setJarByClass(QuerySort_1.class);
      jobSortByRating.setSortComparatorClass(LongWritable.DecreasingComparator.class);

      // JOB SORT BY AVERAGE RATING: MAP CONFIGURATION
      jobSortByRating.setInputFormatClass(SequenceFileInputFormat.class);
      SequenceFileInputFormat.addInputPath(jobSortByRating, staging2);
      jobSortByRating.setMapperClass(Mapper.class);

      // JOB SORT BY AVERAGE RATING: REDUCE CONFIGURATION
      jobSortByRating.setReducerClass(ValueReducer.class);
      jobSortByRating.setNumReduceTasks(SORTING_REDUCE_CARDINALITY);

      // JOB SORT BY AVERAGE RATING: OUTPUT CONFIGURATION
      jobSortByRating.setOutputKeyClass(Text.class);
      jobSortByRating.setOutputValueClass(Text.class);
      FileOutputFormat.setOutputPath(jobSortByRating, output);

      // JOB SORT BY AVERAGE RATING: PARTITIONER CONFIGURATION
      if (SORTING_REDUCE_CARDINALITY > 1) {
        jobSortByRating.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(jobSortByRating.getConfiguration(), parts);
        jobSortByRating.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");
        InputSampler.RandomSampler<Text,Text> sampler = new InputSampler.RandomSampler<>(SORTING_PARTITION_FREQUENCY, SORTING_PARTITION_SAMPLES, SORTING_PARTITION_SPLITS_MAX);
        InputSampler.writePartitionFile(jobSortByRating, sampler);
      }

      // JOB SORT BY AVERAGE RATING: EXECUTION
      code = jobSortByRating.waitForCompletion(true) ? 0 : 1;
    }

    // CLEAN STAGING OUTPUT
    FileSystem fs = FileSystem.get(config);
    fs.delete(staging1, true);
    fs.delete(staging2, true);
    fs.delete(parts, true);

    return code;
  }

  /**
   * The program main method.
   *
   * @param args the program arguments.
   * @throws Exception when the program cannot be executed.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new QuerySort_1(), args);
    System.exit(res);
  }
}
