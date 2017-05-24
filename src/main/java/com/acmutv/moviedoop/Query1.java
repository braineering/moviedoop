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

import com.acmutv.moviedoop.map.MovieFilterByRatingMapper;
import com.acmutv.moviedoop.reduce.MovieMaxRatingReducer;
import com.acmutv.moviedoop.util.DateParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * A MapReduce job that returns movies with rate greater/equal to the specified {@code threshold}
 * and valuated starting from the specified {@code startDate}.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class Query1 {

  /**
   * The job name.
   */
  private static final String JOB_NAME = "Query1";

  /**
   * The job main method.
   *
   * @param args the job arguments.
   * @throws Exception when job cannot be executed.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: Query1 [input] [output] [ratingThreshold] (startDate)");
      System.exit(1);
    }

    final Path input = new Path(args[0]);
    final Path output = new Path(args[1]);
    final Double ratingThreshold = Double.valueOf(args[2]);
    final LocalDateTime startDate = (args.length > 3) ?
        DateParser.parseOrDefault(args[3], LocalDateTime.MIN) : LocalDateTime.MIN;

    System.out.println("Input: " + input);
    System.out.println("Output: " + output);
    System.out.println("Rating Threshold: " + ratingThreshold);
    System.out.println("Start Date: " + DateParser.toString(startDate));

    Configuration config = new Configuration();
    config.setDouble("ratingThreshold", ratingThreshold);
    config.set("startDate", DateParser.toString(startDate));

    Job job = configJob(config);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  /**
   * Configures job.
   *
   * @param config the job configuration.
   * @return the job.
   * @throws IOException when job cannot be configured.
   */
  private static Job configJob(Configuration config) throws IOException {
    Job job = Job.getInstance(config, JOB_NAME);
    job.setJarByClass(Query1.class);
    job.setMapperClass(MovieFilterByRatingMapper.class);
    job.setReducerClass(MovieMaxRatingReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    return job;
  }
}
