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

import com.acmutv.moviedoop.test.map.IdentityMapperText2Text;
import com.acmutv.moviedoop.test.reduce.RatingJoinMovieTitleCachedTextReducerText2Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A map/reduce program that tests TEXT to TEXT to TEXT serialization.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class QuerySerializationText2Text2Text extends Configured implements Tool {

  /**
   * The program name.
   */
  private static final String PROGRAM_NAME = "QuerySerializationText2Text2Text";

  /**
   * The default verbosity.
   */
  private static final boolean VERBOSE = true;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.printf("Usage: %s [-D prop=val] <inRatings> <inMovies> <out>\n", PROGRAM_NAME);
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    // PATHS
    final Path inputRatings = new Path(args[0]);
    final Path inputMovies = new Path(args[1]);
    final Path output = new Path(args[2]);

    // CONTEXT CONFIGURATION
    Configuration config = super.getConf();

    // CONFIGURATION RESUME
    System.out.println("############################################################################");
    System.out.printf("%s\n", PROGRAM_NAME);
    System.out.println("****************************************************************************");
    System.out.println("Input Ratings: " + inputRatings);
    System.out.println("Input Movies: " + inputMovies);
    System.out.println("Output: " + output);
    System.out.println("############################################################################");

    // JOB CONFIGURATION
    Job job = Job.getInstance(config, PROGRAM_NAME);
    job.setJarByClass(QuerySerializationText2Text2Text.class);
    for (FileStatus status : FileSystem.get(config).listStatus(inputMovies)) {
      job.addCacheFile(status.getPath().toUri());
    }

    // MAP CONFIGURATION
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, inputRatings);
    job.setMapperClass(IdentityMapperText2Text.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    // REDUCE CONFIGURATION
    job.setReducerClass(RatingJoinMovieTitleCachedTextReducerText2Text.class);
    job.setNumReduceTasks(2);

    // OUTPUT CONFIGURATION
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, output);

    // JOB EXECUTION
    return job.waitForCompletion(VERBOSE) ? 0 : 1;
  }

  /**
   * The program main method.
   *
   * @param args the program arguments.
   * @throws Exception when the program cannot be executed.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new QuerySerializationText2Text2Text(), args);
    System.exit(res);
  }
}
