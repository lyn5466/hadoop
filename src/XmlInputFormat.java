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
import org.apache.hadoop.mapred.TextInputFormat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;

/**
* Reads records that are delimited by a specifc begin/end tag.
*/
public class XmlInputFormat extends TextInputFormat {

  public static final String START_TAG_KEY = "xmlinput.start";
  public static final String END_TAG_KEY = "xmlinput.end";

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter)
      throws IOException {
    return new XmlRecordReader((FileSplit) inputSplit, jobConf);
  }

  public static class XmlRecordReader implements RecordReader<LongWritable, Text> {
    //private Pattern thePattern;
    //private String theREs;

    private byte[] startTag;
    private byte[] endTag;
    private long start;
    private long end;
    private FSDataInputStream fsin;
    private final DataOutputBuffer buffer = new DataOutputBuffer();

    public XmlRecordReader(FileSplit split, JobConf jobConf) throws IOException {
      startTag = jobConf.get("xmlinput.start").getBytes("utf-8");
      endTag = jobConf.get("xmlinput.end").getBytes("utf-8");
      
      //theREs = "(\\d{18})";
      //thePattern = Pattern.compile(theREs);

      // open the file and seek to the start of the split
      start = split.getStart();
      end = start + split.getLength();
      Path file = split.getPath();
      FileSystem fs = file.getFileSystem(jobConf);
      fsin = fs.open(split.getPath());
      fsin.seek(start);
    }

    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
      if (fsin.getPos() < end) {
        if (readUntilMatch(startTag, false)) {
          try {
            buffer.write(startTag);
            if (readUntilMatch(endTag, true)) {
              key.set(fsin.getPos());
              value.set(buffer.getData(), 0, buffer.getLength());
              return true;
            }
          } finally {
            buffer.reset();
          }
        }
      }
      return false;
    }

    @Override
    public LongWritable createKey() {
      return new LongWritable();
    }

    @Override
    public Text createValue() {
      return new Text();
    }

    @Override
    public long getPos() throws IOException {
      return fsin.getPos();
    }

    @Override
    public void close() throws IOException {
      fsin.close();
    }

    @Override
    public float getProgress() throws IOException {
      return ((fsin.getPos() - start) / (float) (end - start));
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
      int i = 0;
      while (true) {
        int b = fsin.read();
        // end of file:
        if (b == -1) return false;
        // save to buffer:
        if (withinBlock) buffer.write(b);

        // check if we're matching:
        if (b == match[i]) {
          i++;
          if (i >= match.length) return true;
        } else i = 0;
        // see if we've passed the stop point:
        if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
      }
    }
  }
} 