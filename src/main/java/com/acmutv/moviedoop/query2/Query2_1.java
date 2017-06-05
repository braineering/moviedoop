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
package com.acmutv.moviedoop.query2;

import com.acmutv.moviedoop.query2.map.GenresIdentityMapper;
import com.acmutv.moviedoop.query2.map.RatingsMapper;
import com.acmutv.moviedoop.query2.reduce.GenresReducer;
import com.acmutv.moviedoop.query2.reduce.RatingJoinGenreCachedReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A map/reduce program that returns for each genre of the movies with the follow statistics:
 * average and standard deviation of rating.
 * The program leverages inner joins (replication joins as distributed caching on reduce).
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

  /**
   * The default number of reducers for the averaging job.
   */
  private static final int REDUCE_CARDINALITY = 1;

  /**
   * The job main method.
   *
   * @param args the job arguments.
   * @throws Exception when job cannot be executed.
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.printf("Usage: %s <inRatings> <inMovies> <out>\n", PROGRAM_NAME);
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    // USER PARAMETERS
    final Path inputRatings = new Path(args[0]);
    final Path inputMovies = new Path(args[1]);
    final Path output = new Path(args[2]);
    final Path staging = new Path(args[2]+"_staging");

    // CONTEXT CONFIGURATION
    Configuration config = super.getConf();

    // OTHER CONFIGURATION
    final int AVERAGE_REDUCE_CARDINALITY = Integer.valueOf(config.get("moviedoop.average.reduce.cardinality", String.valueOf(REDUCE_CARDINALITY)));
    config.unset("moviedoop.average.reduce.cardinality");

    // CONFIGURATION RESUME
    System.out.println("############################################################################");
    System.out.printf("%s\n", PROGRAM_NAME);
    System.out.println("****************************************************************************");
    System.out.println("Input Ratings: " + inputRatings);
    System.out.println("Input Movies: " + inputMovies);
    System.out.println("Output: " + output);
    System.out.println("----------------------------------------------------------------------------");
    System.out.println("Reduce Cardinality (average): " + REDUCE_CARDINALITY);
    System.out.println("############################################################################");

    /* *********************************************************************************************
     * RATINGS JOIN WITH GENRES BY THE SAME movieId
     **********************************************************************************************/

    // JOB1 CONFIGURATION
    Job job = Job.getInstance(config, PROGRAM_NAME+"_STEP1");
    job.setJarByClass(Query2_1.class);

    for (FileStatus status : FileSystem.get(config).listStatus(inputMovies)) {
      job.addCacheFile(status.getPath().toUri());
    }
    TextInputFormat.addInputPath(job, inputRatings);

    job.setMapperClass(RatingsMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setReducerClass(RatingJoinGenreCachedReducer.class);
    job.setNumReduceTasks(REDUCE_CARDINALITY);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileOutputFormat.setOutputPath(job, staging);

    job.waitForCompletion(true);

    /* *********************************************************************************************
     * GENRES MAPPER AND GENRE'S STATISTICS COMPUTING
     **********************************************************************************************/

    // JOB 2 CONFIGURATION
    Job job2 = Job.getInstance(config, PROGRAM_NAME+"_STEP2");
    job2.setJarByClass(Query2_1.class);

    FileInputFormat.addInputPath(job2, staging);
    job2.setInputFormatClass(SequenceFileInputFormat.class);

    job2.setMapperClass(GenresIdentityMapper.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(DoubleWritable.class);

    job2.setReducerClass(GenresReducer.class);
    job2.setNumReduceTasks(REDUCE_CARDINALITY);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(job2, output);

    // JOB EXECUTION
    return job2.waitForCompletion(true) ? 0 : 1;
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
