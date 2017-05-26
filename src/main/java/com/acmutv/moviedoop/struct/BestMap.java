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

import lombok.Getter;
import lombok.Setter;

import java.util.*;

/**
 * A rank of fixed size.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
@Getter
@Setter
public class BestMap extends HashMap<Long,Double> {

  //private static final Logger LOGGER = LogManager.getLogger(BestMap.class);

  /**
   * Default rank size.
   */
  private static final int DEFAULT_MAX_SIZE = 3;

  /**
   * The rank size.
   */
  protected int maxSize;

  /**
   * The key with the minimum value.
   */
  protected Long minKey;

  /**
   * The minimum value.
   */
  protected Double minValue;

  /**
   * Constructs a new rank.
   */
  public BestMap() {
    super();
    this.maxSize = DEFAULT_MAX_SIZE;
    this.minKey = null;
    this.minValue = Double.MAX_VALUE;
  }

  /**
   * Constructs a new rank.
   *
   * @param maxSize the maximum rank size.
   */
  public BestMap(int maxSize) {
    super();
    this.maxSize = maxSize;
    this.minKey = null;
    this.minValue = Double.MAX_VALUE;
  }

  @Override
  public Double put(Long key, Double value) {
    Double res = null;
    /*
    if (super.containsKey(key)) {
      if (super.get(key) >= value) {
        return null;
      }
      res = super.put(key, value);
      //LOGGER.debug("replaced ({}.{})", key, value);
      if (super.size() >= this.maxSize && key.equals(this.minKey)) {
        this.minValue = Double.MAX_VALUE;
        for (Map.Entry<Long,Double> entry : super.entrySet()) {
          if (entry.getValue() < this.minValue) {
            //LOGGER.debug("Found new minimum ({},{})", entry.getKey(), entry.getValue());
            this.minKey = entry.getKey();
            this.minValue = entry.getValue();
          }
        }
      }
      //LOGGER.debug("min ({},{})", this.minKey, this.minValue);
    } else */
    if (super.size() < this.maxSize) {
      res = super.put(key, value);
      //LOGGER.debug("added ({},{})", key, value);
      if (value < this.minValue) {
        this.minKey = key;
        this.minValue  = value;
        //LOGGER.debug("min ({},{})", this.minKey, this.minValue);
      }
    } else if (value >= this.minValue) {
      super.remove(this.minKey);
      //LOGGER.debug("removed ({})", this.minKey);
      res = super.put(key, value);
      //LOGGER.debug("added ({}.{})", key, value);
      this.minKey = key;
      this.minValue = value;
      //LOGGER.debug("min ({}.{})", this.minKey, this.minValue);
    }
    return res;
  }
}
