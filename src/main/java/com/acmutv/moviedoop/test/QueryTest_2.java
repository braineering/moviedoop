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
package com.acmutv.moviedoop.test;

import com.acmutv.moviedoop.common.input.LinenoSequenceFileInputFormat;
import com.acmutv.moviedoop.test.map.IdentityMapper;
import com.acmutv.moviedoop.test.map.TestMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A map/reduce program that returns movies with rate greater/equal to the specified {@code threshold}
 * and valuated starting from the specified {@code startDate}.
 * The program leverages inner joins (repartition joins).
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class QueryTest_2 extends Configured implements Tool {

  /**
   * The program name.
   */
  private static final String PROGRAM_NAME = "QueryTest_1";

  /**
   * The default verbosity.
   */
  private static final boolean VERBOSE = true;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.printf("Usage: %s [-D prop=val] <inRatings> <out>\n", PROGRAM_NAME);
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    // PATHS
    final Path inputRatings = new Path(args[0]);
    final Path staging = new Path(args[1] + "_staging");
    final Path output = new Path(args[1]);

    // CONTEXT CONFIGURATION
    Configuration config = super.getConf();

    // CONFIGURATION RESUME
    System.out.println("############################################################################");
    System.out.printf("%s\n", PROGRAM_NAME);
    System.out.println("****************************************************************************");
    System.out.println("Input Ratings: " + inputRatings);
    System.out.println("Output: " + output);
    System.out.println("############################################################################");

    // JOB CONFIGURATION
    Job job1 = Job.getInstance(config, PROGRAM_NAME);
    job1.setJarByClass(QueryTest_2.class);

    // MAP CONFIGURATION
    job1.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job1, inputRatings);
    job1.setMapperClass(IdentityMapper.class);

    // REDUCE CONFIGURATION
    job1.setNumReduceTasks(0);

    // OUTPUT CONFIGURATION
    job1.setOutputKeyClass(NullWritable.class);
    job1.setOutputValueClass(Text.class);
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job1, staging);

    // JOB EXECUTION
    int code = job1.waitForCompletion(VERBOSE) ? 0 : 1;

    if (code == 0) {
      // JOB CONFIGURATION
      Job job2 = Job.getInstance(config, PROGRAM_NAME);
      job2.setJarByClass(QueryTest_2.class);

      // MAP CONFIGURATION
      job2.setInputFormatClass(LinenoSequenceFileInputFormat.class);
      LinenoSequenceFileInputFormat.addInputPath(job2, staging);
      job2.setMapperClass(TestMapper.class);
      //job.setMapOutputKeyClass(NullWritable.class);
      //job.setMapOutputValueClass(Text.class);

      // REDUCE CONFIGURATION
      //job.setReducerClass(TestReducer.class);
      job2.setNumReduceTasks(0);

      // OUTPUT CONFIGURATION
      job2.setOutputKeyClass(NullWritable.class);
      job2.setOutputValueClass(Text.class);
      job2.setOutputFormatClass(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(job2, output);

      // JOB EXECUTION
      code = job2.waitForCompletion(VERBOSE) ? 0 : 1;
    }
    return code;
  }

  /**
   * The program main method.
   *
   * @param args the program arguments.
   * @throws Exception when the program cannot be executed.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new QueryTest_2(), args);
    System.exit(res);
  }
}
