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
package com.acmutv.moviedoop.common.util;

import org.junit.Assert;
import org.junit.Test;

import java.time.*;

/**
 * Unit test for {@link DateParser}.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class DateParserTest {

  /**
   * Tests the date and time parsing, with fallback.
   */
  @Test
  public void test_parseOrDefault() {
    String line = null;

    @SuppressWarnings("ConstantConditions") LocalDateTime actual = DateParser.parseOrDefault(line, LocalDateTime.MIN);

    LocalDateTime expected = LocalDateTime.MIN;

    Assert.assertEquals(expected, actual);
  }

  /**
   * Tests the date parsing.
   */
  @Test
  public void test_parseDate() {
    String line = "27/06/1990";

    LocalDateTime actual = DateParser.parse(line);

    LocalDateTime expected = LocalDateTime.of(1990, Month.JUNE, 27, 0, 0, 0);

    Assert.assertEquals(expected, actual);
  }

  /**
   * Tests the date and time parsing.
   */
  @Test
  public void test_parseDateTime() {
    String line = "27/06/1990T12:12:12";

    LocalDateTime actual = DateParser.parse(line);

    LocalDateTime expected = LocalDateTime.of(1990, Month.JUNE, 27, 12, 12, 12);

    Assert.assertEquals(expected, actual);
  }

  /**
   * Tests the date stringify.
   */
  @Test
  public void test_tostring() {
    LocalDateTime date = LocalDateTime.of(1990, Month.JUNE, 27, 0, 0, 0);

    String actual = DateParser.toString(date);

    String expected = "27/06/1990T00:00:00";

    Assert.assertEquals(expected, actual);
  }

  /**
   * Tests the date stringify.
   * The minimum value (01/01/0T00:00:00).
   */
  @Test
  public void test_tostring_min() {
    LocalDateTime date = DateParser.MIN;

    String actual = DateParser.toString(date);

    String expected = "01/01/0001T00:00:00";

    Assert.assertEquals(expected, actual);
  }

  /**
   * Tests the date stringify.
   * The maximum value (31/12/+999999999T23:59:59).
   */
  @Test
  public void test_tostring_max() {
    LocalDateTime date = DateParser.MAX;

    String actual = DateParser.toString(date);

    String expected = "31/12/+999999999T23:59:59";

    Assert.assertEquals(expected, actual);
  }

  /**
   * Tests the date stringify.
   * Null case.
   */
  @Test
  public void test_tostring_null() {
    LocalDateTime date = null;

    @SuppressWarnings("ConstantConditions") String actual = DateParser.toString(date);

    String expected = null;

    //noinspection ConstantConditions
    Assert.assertEquals(expected, actual);
  }
}
