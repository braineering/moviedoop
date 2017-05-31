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

import com.acmutv.moviedoop.query1.Query1_1;
import com.acmutv.moviedoop.query1.Query1_2;
import com.acmutv.moviedoop.query1.Query1_3;
import com.acmutv.moviedoop.query1.Query1_4;
import com.acmutv.moviedoop.query3.Query3_1;
import com.acmutv.moviedoop.query3.Query3_2;
import com.acmutv.moviedoop.test.*;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.log4j.Logger;

/**
 * The main driver.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class MoviedoopDriver {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger(MoviedoopDriver.class);

  /**
   * The driver main method.
   *
   * @param args the job arguments.
   */
  public static void main(String[] args) {
    int exitCode = -1;
    ProgramDriver driver = new ProgramDriver();
    try {
      driver.addClass("query1_1", Query1_1.class,
          "A map/reduce program that selects movies according to their rating and period. " +
              "The program leverages inner joins implemented with repartition pattern.");

      driver.addClass("query1_2", Query1_2.class,
          "A map/reduce program that selects movies according to their rating and period. " +
              "The program leverages inner joins implemented with replication pattern (reduce).");

      driver.addClass("query1_3", Query1_3.class,
          "A map/reduce program that selects movies according to their rating and period. " +
              "The program leverages inner joins implemented with replication pattern (map).");

      driver.addClass("query1_4", Query1_4.class,
          "A map/reduce program that selects movies according to their rating and period. " +
              "The program leverages inner joins implemented with replication pattern (map).");

      driver.addClass("query3_1", Query3_1.class,
          "A map/reduce program that returns the comparison between " +
              "(i) the top-`rankSize` movies, considering average ratings in period from `ratingTimestampTopKLB` \n" +
              " and `ratingTimestampTopKUB`; and\n" +
              " (ii) the total rank of moviues, considering average ratings in period from `ratingTimestampRankLB`\n" +
              " and `ratingTimestampRankUB`.\n" +
              " It leverages BestMap.");

      driver.addClass("query3_2", Query3_2.class,
          "A map/reduce program that returns the comparison between " +
              "(i) the top-`rankSize` movies, considering average ratings in period from `ratingTimestampTopKLB` \n" +
              " and `ratingTimestampTopKUB`; and\n" +
              " (ii) the total rank of moviues, considering average ratings in period from `ratingTimestampRankLB`\n" +
              " and `ratingTimestampRankUB`.\n" +
              " It leverages BestMap.");

      driver.addClass("query_sort_1", QuerySort_1.class,
          "A map/reduce program that sorts movies according to their average rating and period. " +
              "The program leverages the total sorting pattern.");

      driver.addClass("query_topk_1", QueryTopK_1.class,
          "A map/reduce program that calculates top-K movies according to their average rating and period. " +
              "The program leverages TreeMap to build the ranking..");

      driver.addClass("query_topk_2", QueryTopK_2.class,
          "A map/reduce program that calculates top-K movies according to their average rating and period. " +
              "The program leverages BestMap to build the ranking.");

      driver.addClass("query_test_1", QueryTest_1.class,
          "A map/reduce program to make experiments and tests");

      driver.addClass("query_test_2", QueryTest_2.class,
          "A map/reduce program to make experiments and tests");

      exitCode = driver.run(args);
    } catch (Throwable exc) {
      exc.printStackTrace();
      LOG.error(exc.getMessage());
    }

    System.exit(exitCode);
  }
}
