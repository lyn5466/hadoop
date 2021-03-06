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

public class PatternInputFormat
        extends FileInputFormat {
    
    @Override
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter)
      throws IOException {
    return new PatternRecordReader((FileSplit) inputSplit, jobConf);
  }
}
