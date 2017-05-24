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
package com.acmutv.moviedoop.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility to parse records.
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class RecordParser {

  /**
   * The input record attributes for the `rating` data set.
   */
  public static final String[] RATING_FIELDS = {"userId","movieId","rating","timestamp"};

  /**
   * The input record attributes for the intermediate `rank` data set.
   */
  public static final String[] RANK_FIELDS = {"movieId","rating"};

  /**
   * The input record delimiter for the `rating` data set.
   */
  public static final String DELIMITER = ",";

  /**
   * Parses {@code line} as a list of attributes separated by {@code delimiter}.
   *
   * @param line the string to parse.
   * @param attributes the list of attributes.
   * @param delimiter the string delimiter.
   * @return the map of attributes.
   */
  public static Map<String,String> parse(String line, String[] attributes, String delimiter) {
    Map<String,String> map = new HashMap<>();
    String[] values = line.split(delimiter);
    for (int i = 0; i < values.length; i++) {
      map.put(attributes[i], values[i]);
    }
    return map;
  }
}
