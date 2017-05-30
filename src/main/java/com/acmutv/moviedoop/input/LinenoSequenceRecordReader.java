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
package com.acmutv.moviedoop.input;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import java.io.IOException;

/**
 * A record reader for sequence files that emits the record number as key and the record content as
 * value.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 * @see LinenoSequenceFileInputFormat
 */
public class LinenoSequenceRecordReader extends RecordReader<LongWritable, Text> {

  private LongWritable key;
  private Text value;
  private SequenceFileRecordReader<LongWritable, Text> rr;
  private long lineno;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    this.rr = new SequenceFileRecordReader<>();
    this.rr.initialize(inputSplit, taskAttemptContext);
    this.lineno = 1;
    this.key = new LongWritable();
    this.value = new Text();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!this.rr.nextKeyValue()) {
      return false;
    }
    this.key.set(this.lineno);
    this.value.set(this.rr.getCurrentValue());

    this.lineno++;

    return true;
  }

  @Override
  public LongWritable getCurrentKey() {
    return this.key;
  }

  @Override
  public Text getCurrentValue() {
    return this.value;
  }

  @Override
  public float getProgress() throws IOException {
    return this.rr.getProgress();
  }

  @Override
  public void close() throws IOException {
    if (null != this.rr) {
      this.rr.close();
      this.rr = null;
    }

    this.key = null;
    this.value = null;
  }
}
