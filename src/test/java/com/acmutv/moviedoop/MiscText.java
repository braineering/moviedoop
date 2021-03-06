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

import org.junit.Test;

/**
 * This class realizes ...
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class MiscText {

  @Test
  public void test() {
    String str = "1:10";
    String parts[] = str.split(":");
    String header[] = parts[0].split(";",-1);
    boolean is1 = header.length >= 1 && header[0].equals("1");
    boolean is2 = header.length >= 2 && header[1].equals("2");
    double val = Double.valueOf(parts[1]);

    System.out.printf("is1=%b | is2=%b | value=%f\n", is1, is2, val);

    str = "1;2:10";
    parts = str.split(":");
    header = parts[0].split(";",-1);
    is1 = header.length >= 1 && header[0].equals("1");
    is2 = header.length >= 2 && header[1].equals("2");
    val = Double.valueOf(parts[1]);

    System.out.printf("is1=%b | is2=%b | value=%f\n", is1, is2, val);
  }
}
