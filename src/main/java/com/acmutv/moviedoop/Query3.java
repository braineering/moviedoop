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

import com.acmutv.moviedoop.map.MovieTopKWithinPeriodMapper;
import com.acmutv.moviedoop.reduce.MovieTopKReducer;
import com.acmutv.moviedoop.util.DateParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDate;

/**
 * A MapReduce job that returns the top-{@code rank} movies for the period from {@code startDate1} to
 * {@code endDate1} and their rating variation with respect to the classification in period from
 * {@code startDate2} to {@code endDate2}.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class Query3 {

  /**
   * The job name.
   */
  private static final String JOB_NAME = "Query3";

  /**
   * The job main method.
   *
   * @param args the job arguments.
   * @throws Exception when job cannot be executed.
   */
  public static void main(String[] args) throws Exception {
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    Integer rankSize = Integer.valueOf(args[2]);
    LocalDate startDate1 = DateParser.parse(args[3]);
    LocalDate endDate1 = DateParser.parse(args[4]);
    LocalDate startDate2 = DateParser.parse(args[5]);
    LocalDate endDate2 = DateParser.parse(args[6]);

    System.out.println("Input: " + inputPath);
    System.out.println("Output: " + outputPath);
    System.out.println("rankSize: " + rankSize);
    System.out.println("Start Date 1: " + DateParser.toString(startDate1));
    System.out.println("End Date 1: " + DateParser.toString(endDate1));
    System.out.println("Start Date 2: " + DateParser.toString(startDate2));
    System.out.println("End Date 2: " + DateParser.toString(endDate2));

    Configuration config = new Configuration();
    config.setInt("rankSize", rankSize);
    config.set("startDate1", DateParser.toString(startDate1));
    config.set("endDate1", DateParser.toString(endDate1));
    config.set("startDate2", DateParser.toString(startDate2));
    config.set("endDate2", DateParser.toString(endDate2));

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
    job.setJarByClass(Query3.class);
    job.setMapperClass(MovieTopKWithinPeriodMapper.class);
    job.setReducerClass(MovieTopKReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    return job;
  }
}
