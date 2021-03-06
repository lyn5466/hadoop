/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;


public class PatternRecordReader implements RecordReader<LongWritable, Text> {
  private static final Log LOG
    = LogFactory.getLog(PatternRecordReader.class.getName());

  private final static Text EOL = new Text("\n");
  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private LineReader in;
  int maxLineLength;

  private Pattern delimiterPattern;
  private String delimiterRegex = "^[A-Za-z]{3},\\s\\d{2}\\s[A-Za-z]{3}.*";


  public PatternRecordReader(Configuration job, 
                          FileSplit split) throws IOException {
    // this.delimiterRegex = job.get("record.delimiter.regex");
    // System.out.println(delimiterRegex);
    delimiterPattern = Pattern.compile(delimiterRegex);
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                    Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    boolean skipFirstLine = false;
    if (codec != null) {
      in = new LineReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
        skipFirstLine = true;
        --start;
        fileIn.seek(start);
      }
      in = new LineReader(fileIn, job);
    }
    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += in.readLine(new Text(), 0,
                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
  }
  
  public LongWritable createKey() {
    return new LongWritable();
  }
  
  public Text createValue() {
    return new Text();
  }
  
  /** Read a line. */
  public synchronized boolean next(LongWritable key, Text value)
    throws IOException {

    while (pos < end) {
      key.set(pos);

      int newSize = readNext(value, maxLineLength,
                                Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                                         maxLineLength));
      if (newSize == 0) {
        return false;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
        return true;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
    }

    return false;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public  synchronized long getPos() throws IOException {
    return pos;
  }

  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }
  private int readNext(Text text,
                        int maxLineLength,
                        int maxBytesToConsume)
            throws IOException {
 
        int offset = 0;
        text.clear();
        Text tmp = new Text();
 
        for (int i = 0; i < maxBytesToConsume; i++) {
 
            int offsetTmp = in.readLine(
                                     tmp,
                                     maxLineLength,
                                     maxBytesToConsume);
            offset += offsetTmp;
            Matcher m = delimiterPattern.matcher(tmp.toString());
 
            // End of File
            if (offsetTmp == 0) {
                break;
            }
 
            if (m.matches()) {
		System.out.println(tmp.toString());
                // Record delimiter
                break;
            } else {
                // Append value to record
                text.append(EOL.getBytes(), 0, EOL.getLength());
                text.append(tmp.getBytes(), 0, tmp.getLength());
            }
        }
        return offset;
    }

}
