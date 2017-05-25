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

import com.acmutv.moviedoop.map.MoviesTopKWithinPeriodMapper;
import com.acmutv.moviedoop.reduce.MoviesTopKReducer;
import com.acmutv.moviedoop.util.DateParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.time.LocalDateTime;

/**
 * A map/reduce program that returns the top-`rankSize` movies for the period from `ratingTimestampLB`
 * and `ratingTimestampUB`.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class QueryTopK extends Configured implements Tool {

  /**
   * The program name.
   */
  private static final String PROGRAM_NAME = "QueryTopK";

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("Usage: QueryTopK [input] [output] [rankSize] (ratingTimestampLB) (ratingTimestampUB)");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    // USER PARAMETERS
    final Path input = new Path(args[0]);
    final Path output = new Path(args[1]);
    final Integer rankSize = Integer.valueOf(args[2]);
    LocalDateTime ratingTimestampLB = (args.length > 3) ?
        DateParser.parseOrDefault(args[3], DateParser.MIN) : DateParser.MIN;
    LocalDateTime ratingTimestampUB = (args.length > 4) ?
        DateParser.parseOrDefault(args[4], DateParser.MAX) : DateParser.MAX;

    // USER PARAMETERS RESUME
    System.out.println("############################################################################");
    System.out.printf("%s\n", PROGRAM_NAME);
    System.out.println("****************************************************************************");
    System.out.println("Input: " + input);
    System.out.println("Output: " + output);
    System.out.println("Movie Rank Size: " + rankSize);
    System.out.println("Movie Rating Timestamp Lower Bound: " + DateParser.toString(ratingTimestampLB));
    System.out.println("Movie Rating Timestamp Upper Bound: " + DateParser.toString(ratingTimestampUB));
    System.out.println("############################################################################");

    // CONTEXT CONFIGURATION
    Configuration config = new Configuration();
    config.setInt("movie.rank.size", rankSize);
    config.setLong("movie.rating.timestamp.lb", DateParser.toSeconds(ratingTimestampLB));
    config.setLong("movie.rating.timestamp.ub", DateParser.toSeconds(ratingTimestampUB));

    // JOB CONFIGURATION
    Job job = Job.getInstance(config, PROGRAM_NAME);
    job.setJarByClass(QueryTopK.class);

    // MAP CONFIGURATION
    FileInputFormat.addInputPath(job, input);
    job.setMapperClass(MoviesTopKWithinPeriodMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    // REDUCE CONFIGURATION
    job.setReducerClass(MoviesTopKReducer.class);
    job.setNumReduceTasks(1);

    // OUTPUT CONFIGURATION
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
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
    int res = ToolRunner.run(new Configuration(), new Query1_1(), args);
    System.exit(res);
  }
}
