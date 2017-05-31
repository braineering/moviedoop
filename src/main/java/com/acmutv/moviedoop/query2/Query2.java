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

import com.acmutv.moviedoop.query2.map.RatingsMapper;
import com.acmutv.moviedoop.query2.reduce.Query2RatingJoinGenreCachedReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * A MapReduce job that returns movies with rate greater/equal to the specified {@code threshold}
 * and valuated starting from the specified {@code startDate}.
 * The job leverages inner joins (repartition joins).
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class Query2 {

  /**
   * The part1 job name.
   */
  private static final String JOB_NAME = "Query2";

  /**
   * The job main method.
   *
   * @param args the job arguments.
   * @throws Exception when job cannot be executed.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: Query2 [inputRatings] [inputMovies] [output]");
      System.exit(1);
    }

    // USER PARAMETERS
    final Path inputRatings = new Path(args[0]);
    final Path inputMovies = new Path(args[1]);
    final Path output = new Path(args[2]);


    System.out.println("Input Ratings: " + inputRatings);
    System.out.println("Input Movies: " + inputMovies);
    System.out.println("Output: " + output);

    // CONTEXT CONFIGURATION
    Configuration config = new Configuration();

    // JOB CONFIGURATION
    Job job = Job.getInstance(config, JOB_NAME);
    job.setJarByClass(Query2.class);

   /* // MAPPERS CONFIGURATION
    MultipleInputs.addInputPath(job, inputRatings, TextInputFormat.class, RatingsMapper.class);
    MultipleInputs.addInputPath(job, inputMovies, TextInputFormat.class, GenresMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    // REDUCERS CONFIGURATION
    job.setReducerClass(RatingsGenresJoinReducer.class);
    job.setNumReduceTasks(1);

    // OUTPUT CONFIGURATION
    FileOutputFormat.setOutputPath(job, output);

    // JOB EXECUTION
    System.exit(job.waitForCompletion(true) ? 0 : 1);*/

    for (FileStatus status : FileSystem.get(config).listStatus(inputRatings)) {
      job.addCacheFile(status.getPath().toUri());
    }

    // MAP CONFIGURATION
    FileInputFormat.addInputPath(job, inputMovies);
    job.setMapperClass(RatingsMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    // REDUCE CONFIGURATION
    job.setReducerClass(Query2RatingJoinGenreCachedReducer.class);
    job.setNumReduceTasks(1);

    // OUTPUT CONFIGURATION
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileOutputFormat.setOutputPath(job, output);

    // JOB EXECUTION
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
