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
package com.acmutv.moviedoop.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The record representing a genre.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
public class GenreWritable implements WritableComparable<GenreWritable> {

  /**
   * The genre name.
   */
  public String title;

  /**
   * The average of rating for a genre
   */
  public Double average;

  /**
   * The standard deviation of rating for a genre
   */
  public Double stdDev;

  /**
   * Reads the fields.
   *
   * @param in the input.
   * @throws IOException when the input cannot be read.
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.title = in.readUTF();
    this.average = in.readDouble();
    this.stdDev = in.readDouble();
  }

  /**
   * Writes the fields.
   *
   * @param out the output.
   * @throws IOException when the output cannot be written.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.title);
    out.writeDouble(this.average);
    out.writeDouble(this.stdDev);
  }

  /**
   * Returns the string representation.
   * @return the string representation.
   */
  @Override
  public String toString() {
    return String.format("%d,%f,%f", this.title, this.average, this.stdDev);
  }

  /**
   * Compares this object with the specified object for order.  Returns a
   * negative integer, zero, or a positive integer as this object is less
   * than, equal to, or greater than the specified object.
   * <p>
   * <p>The implementor must ensure <tt>sgn(x.compareTo(y)) ==
   * -sgn(y.compareTo(x))</tt> for all <tt>x</tt> and <tt>y</tt>.  (This
   * implies that <tt>x.compareTo(y)</tt> must throw an exception iff
   * <tt>y.compareTo(x)</tt> throws an exception.)
   * <p>
   * <p>The implementor must also ensure that the relation is transitive:
   * <tt>(x.compareTo(y)&gt;0 &amp;&amp; y.compareTo(z)&gt;0)</tt> implies
   * <tt>x.compareTo(z)&gt;0</tt>.
   * <p>
   * <p>Finally, the implementor must ensure that <tt>x.compareTo(y)==0</tt>
   * implies that <tt>sgn(x.compareTo(z)) == sgn(y.compareTo(z))</tt>, for
   * all <tt>z</tt>.
   * <p>
   * <p>It is strongly recommended, but <i>not</i> strictly required that
   * <tt>(x.compareTo(y)==0) == (x.equals(y))</tt>.  Generally speaking, any
   * class that implements the <tt>Comparable</tt> interface and violates
   * this condition should clearly indicate this fact.  The recommended
   * language is "Note: this class has a natural ordering that is
   * inconsistent with equals."
   * <p>
   * <p>In the foregoing description, the notation
   * <tt>sgn(</tt><i>expression</i><tt>)</tt> designates the mathematical
   * <i>signum</i> function, which is defined to return one of <tt>-1</tt>,
   * <tt>0</tt>, or <tt>1</tt> according to whether the value of
   * <i>expression</i> is negative, zero or positive.
   *
   * @param o the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object
   * is less than, equal to, or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException   if the specified object's type prevents it
   *                              from being compared to this object.
   */
  @Override
  public int compareTo(GenreWritable o) {return this.title.compareTo(o.title);
  }

}
