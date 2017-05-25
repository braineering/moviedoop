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

import org.apache.hadoop.util.ProgramDriver;

/**
 * The main driver.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class MoviedoopDriver {

  /**
   * The driver main method.
   *
   * @param args the job arguments.
   * @throws Exception when job cannot be executed.
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

      driver.addClass("query_sort", QuerySort.class,
          "A map/reduce program that sorts movies according to their average rating and period. " +
              "The program leverages inner joins implemented with replication pattern (reduce).");

      driver.addClass("query_topk", QuerySort.class,
          "A map/reduce program that calculates top-K movies according to their average rating and period. " +
              "The program leverages inner joins implemented with replication pattern (reduce).");


      exitCode = driver.run(args);
    } catch (Throwable exc) {
      exc.printStackTrace();
    }

    System.exit(exitCode);
  }
}
