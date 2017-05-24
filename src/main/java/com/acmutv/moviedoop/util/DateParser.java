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

import org.apache.commons.lang.ObjectUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;

/**
 * Utility to parse dates.
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class DateParser {

  /**
   * The date and time format (dd/mm/yyyyThh:mm:ss).
   */
  private static DateTimeFormatter DATE_TIME_FORMAT =
      new DateTimeFormatterBuilder().appendPattern("dd/MM/yyyy['T'[HH][:mm][:ss]]")
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
          .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
          .toFormatter();

  public static final LocalDateTime MIN = LocalDateTime.of(0,1,1,0,0,0);

  public static final LocalDateTime MAX = LocalDateTime.MAX;

  /**
   * Parses {@code line} as date and time (i.e. dd/mm/yyyy, dd/mm/yyyyThh:mm:ss).
   *
   * @param line the string to parse.
   * @return the date;
   * @throws DateTimeParseException when date and time cannot be parsed.
   */
  public static LocalDateTime parse(String line) {
    return LocalDateTime.parse(line, DATE_TIME_FORMAT);
  }

  /**
   * Parses {@code line} as date and time (i.e. dd/mm/yyyt, dd/mm/yyyyThh:mm:ss), with the specified
   * fallback value.
   *
   * @param line the string to parse.
   * @param fallback the fallback value.
   * @return the date; default if error.
   */
  public static LocalDateTime parseOrDefault(String line, LocalDateTime fallback) {
    try {
      return LocalDateTime.parse(line, DATE_TIME_FORMAT);
    } catch (DateTimeParseException|NullPointerException exc) {
      return fallback;
    }
  }

  /**
   * Stringify {@code date} (i.e. dd/mm/yyy).
   *
   * @param date the date to stringify.
   * @return the date as string; null if error.
   */
  public static String toString(LocalDateTime date) {
    try {
      return date.format(DATE_TIME_FORMAT);
    } catch (NullPointerException exc) {
      return null;
    }

  }
}
