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

import com.acmutv.moviedoop.map.FilterRatingsByScoreAndTimestampMapper;
import com.acmutv.moviedoop.reduce.MaxRatingReducer;
import com.acmutv.moviedoop.util.DateParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.time.LocalDateTime;

/**
 * A MapReduce job that returns movies with rate greater/equal to the specified {@code threshold}
 * and valuated starting from the specified {@code startDate}.
 * The job does not leverage inner joins.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class Query1_1 {

  /**
   * The job name.
   */
  private static final String JOB_NAME = "Query1_1";

  /**
   * The job main method.
   *
   * @param args the job arguments.
   * @throws Exception when job cannot be executed.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: Query1_1 [inputRatings] [output] [ratingThreshold] (startDate)");
      System.exit(1);
    }

    // USER PARAMETERS
    final Path input = new Path(args[0]);
    final Path output = new Path(args[1]);
    final Double ratingThreshold = Double.valueOf(args[2]);
    final LocalDateTime startDate = (args.length > 3) ?
        DateParser.parseOrDefault(args[3], DateParser.MIN) : DateParser.MIN;

    // USER PARAMETERS RESUME
    System.out.println("Input: " + input);
    System.out.println("Output: " + output);
    System.out.println("Rating Threshold: " + ratingThreshold);
    System.out.println("Start Date: " + DateParser.toString(startDate));

    // CONTEXT CONFIGURATION
    Configuration config = new Configuration();
    config.setDouble("ratingThreshold", ratingThreshold);
    config.setLong("startDate", DateParser.toSeconds(startDate));

    // JOB CONFIGURATION
    Job job = Job.getInstance(config, JOB_NAME);
    job.setJarByClass(Query1_1.class);

    // MAP CONFIGURATION
    FileInputFormat.addInputPath(job, input);
    job.setMapperClass(FilterRatingsByScoreAndTimestampMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    // REDUCE CONFIGURATION
    job.setReducerClass(MaxRatingReducer.class);
    job.setNumReduceTasks(1);

    // OUTPUT CONFIGURATION
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileOutputFormat.setOutputPath(job, output);

    // JOB EXECUTION
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
