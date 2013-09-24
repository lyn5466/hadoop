import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.TaskAttemptContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.util.LineReader;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;

class ObjPosRecordReader implements RecordReader<Text, IdTime> {

  private LineRecordReader lineReader;
  private LongWritable lineKey;
  private Text lineValue;

  public ObjPosRecordReader(JobConf job, FileSplit split) throws IOException {
    lineReader = new LineRecordReader(job, split);

    lineKey = lineReader.createKey();
    lineValue = lineReader.createValue();
  }

  public boolean next(Text key, IdTime value) throws IOException {
    // get the next line
    if (!lineReader.next(lineKey, lineValue)) {
      return false;
    }

    // parse the lineValue which is in the format:
    // objName, x, y, z
    String [] pieces = lineValue.toString().split("\\|");
    
    /*
    if (pieces.length != 4) {
      throw new IOException("Invalid record received");
    }
    */
    while (pieces.length != 4) {
	if (!lineReader.next(lineKey, lineValue)) {
      	    return false;
    	}
	pieces = lineValue.toString().split("\\|");
    }

    // try to parse floating point components of value
    /*
    float fx, fy, fz;
    try {
      fx = Float.parseFloat(pieces[1].trim());
      fy = Float.parseFloat(pieces[2].trim());
      fz = Float.parseFloat(pieces[3].trim());
    } catch (NumberFormatException nfe) {
      throw new IOException("Error parsing floating point value in record");
    }

    // now that we know we'll succeed, overwrite the output objects

    key.set(pieces[0].trim()); // objName is the output key.

    value.x = fx;
    value.y = fy;
    value.z = fz;
    */

    key.set(pieces[3]);
    value.Id = Long.parseLong(pieces[1]);
    value.myTime = pieces[2];
    //value.set(pieces[2]);
    return true;
  }

  public Text createKey() {
    return new Text("");
  }

  public IdTime createValue() {
    return new IdTime();
  }

  /*
  public Text createValue() {
    return new Text("");
  }
  */

  public long getPos() throws IOException {
    return lineReader.getPos();
  }

  public void close() throws IOException {
    lineReader.close();
  }

  public float getProgress() throws IOException {
    return lineReader.getProgress();
  }
}
