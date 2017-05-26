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
package com.acmutv.moviedoop.struct;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link BestMap}.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class BestMapTest {

  /**
   * Tests the simple case of rank construction.
   */
  @Test
  public void test_duplicate() {
    BestMap actual = new BestMap(3);
    actual.put(1L, 5.0);
    actual.put(1L, 10.0);
    actual.put(1L, 15.0);
    actual.put(1L, 10.0);
    actual.put(1L, 5.0);
    actual.put(1L, 1.0);

    BestMap expected = new BestMap(3);
    expected.put(1L, 1.0);

    Assert.assertEquals(expected, actual);
  }

  /**
   * Tests the simple case of rank construction.
   * 1,1,1.0,02/01/1970
   2,1,2.0,02/01/1970
   3,1,3.0,02/01/1970
   1,2,1.0,02/01/1970
   2,2,2.0,02/01/1970
   3,2,3.0,02/01/1970
   1,3,1.0,02/01/1970
   2,3,2.0,02/01/1970
   3,3,3.0,02/01/1970
   4,3,4.0,02/01/1970
   1,4,1.0,02/01/1970
   2,4,2.0,02/01/1970
   3,4,3.0,02/01/1970
   4,4,4.0,02/01/1970
   5,4,5.0,02/01/1970
   6,4,5.0,02/01/1970
   1,5,1.0,02/01/1970
   2,5,2.0,02/01/1970
   3,5,3.0,02/01/1970
   4,5,4.0,02/01/1970
   5,5,5.0,02/01/1970
   6,5,5.0,02/01/1970
   */
  @Test
  public void test_simple() {
    BestMap actual = new BestMap(3);
    actual.put(1L, 1.0);
    actual.put(1L, 2.0);
    actual.put(1L, 3.0);
    actual.put(2L, 1.0);
    actual.put(2L, 2.0);
    actual.put(2L, 3.0);
    actual.put(3L, 1.0);
    actual.put(3L, 2.0);
    actual.put(3L, 3.0);
    actual.put(3L, 4.0);
    actual.put(4L, 1.0);
    actual.put(4L, 2.0);
    actual.put(4L, 3.0);
    actual.put(4L, 4.0);
    actual.put(4L, 5.0);
    actual.put(4L, 5.0);
    actual.put(5L, 1.0);
    actual.put(5L, 2.0);
    actual.put(5L, 3.0);
    actual.put(5L, 4.0);
    actual.put(5L, 5.0);
    actual.put(5L, 5.0);
    actual.put(1L, 1.0);
    actual.put(2L, 1.0);
    actual.put(3L, 1.0);
    actual.put(4L, 1.0);
    actual.put(5L, 1.0);

    BestMap expected = new BestMap(3);
    expected.put(4L, 5.0);
    expected.put(3L, 4.0);
    expected.put(2L, 3.0);

    Assert.assertEquals(expected, actual);
  }
}
