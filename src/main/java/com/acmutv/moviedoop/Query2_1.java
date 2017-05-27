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

import com.acmutv.moviedoop.map.GenresMapper;
import com.acmutv.moviedoop.map.RatingsMapper;
import com.acmutv.moviedoop.reduce.RatingsGenresJoinReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A MapReduce job that returns movies with rate greater/equal to the specified {@code threshold}
 * and valuated starting from the specified {@code startDate}.
 * The job leverages inner joins (repartition joins).
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class Query2_1 extends Configured implements Tool {

  /**
   * The program name.
   */
  private static final String PROGRAM_NAME = "Query2_1";

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.printf("Usage: %s [-D prop=val] <inRatings> <inMovies> <out>\n", PROGRAM_NAME);
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    // USER PARAMETERS
    final Path inputRatings = new Path(args[0]);
    final Path inputMovies = new Path(args[1]);
    final Path output = new Path(args[2]);

    // CONTEXT CONFIGURATION
    Configuration config = super.getConf();

    // USER PARAMETERS RESUME
    System.out.println("############################################################################");
    System.out.printf("%s\n", PROGRAM_NAME);
    System.out.println("****************************************************************************");
    System.out.println("Input Ratings: " + inputRatings);
    System.out.println("Input Movies: " + inputMovies);
    System.out.println("Output: " + output);
    System.out.println("############################################################################");

    // JOB CONFIGURATION
    Job job = Job.getInstance(config, PROGRAM_NAME);
    job.setJarByClass(Query1_1.class);

    // MAP CONFIGURATION
    MultipleInputs.addInputPath(job, inputRatings, TextInputFormat.class, RatingsMapper.class);
    MultipleInputs.addInputPath(job, inputMovies, TextInputFormat.class, GenresMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    //job.setMapOutputValueClass(Text.class);

    job.setMapOutputValueClass(DoubleWritable.class);

    // REDUCE CONFIGURATION
    //job.setReducerClass(RatingsGenresJoinReducer.class);
    //job.setNumReduceTasks(1);

    // OUTPUT CONFIGURATION
    //job.setOutputKeyClass(Text.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileOutputFormat.setOutputPath(job, output);

    // JOB EXECUTION
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * The program main method.
   *
   * @param args the program arguments.
   * @throws Exception when the program cannot be executed.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Query2_1(), args);
    System.exit(res);
  }
}
