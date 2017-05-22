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

import com.acmutv.moviedoop.map.TokenizationMapper;
import com.acmutv.moviedoop.reduce.SumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
    Integer rank = Integer.valueOf(args[2]);
    Date startDate1 = new SimpleDateFormat("dd/MM/yy").parse(args[3]);
    Date endDate1 = new SimpleDateFormat("dd/MM/yy").parse(args[4]);
    Date startDate2 = new SimpleDateFormat("dd/MM/yy").parse(args[5]);
    Date endDate2 = new SimpleDateFormat("dd/MM/yy").parse(args[6]);

    Configuration config = new Configuration();
    config.setInt("rank", rank);
    config.set("startDate1", startDate1.toString());
    config.set("endDate1", endDate1.toString());
    config.set("startDate2", startDate2.toString());
    config.set("endDate2", endDate2.toString());

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
    job.setMapperClass(TokenizationMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job;
  }
}
