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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDate;

/**
 * A MapReduce job that joins movies with their rating.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class JoinMovieRating {

  /**
   * The job name.
   */
  private static final String JOB_NAME = "JoinMovieRating";

  /**
   * The job main method.
   *
   * @param args the job arguments.
   * @throws Exception when job cannot be executed.
   */
  public static void main(String[] args) throws Exception {
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    Double ratingThreshold = Double.valueOf(args[2]);
    LocalDate startDate = DateParser.parse(args[3]);

    System.out.println("Input: " + inputPath);
    System.out.println("Output: " + outputPath);
    System.out.println("Rating Threshold: " + ratingThreshold);
    System.out.println("Start Date: " + DateParser.toString(startDate));

    Configuration config = new Configuration();
    config.setDouble("ratingThreshold", ratingThreshold);
    config.set("startDate", DateParser.toString(startDate));

    Job job = configJob(config);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

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
    job.setJarByClass(JoinMovieRating.class);
    job.setMapperClass(MovieFilterByRatingMapper.class);
    job.setReducerClass(MovieMaxRatingReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    return job;
  }
}
