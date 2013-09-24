import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Hw1_3Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private final static Text location = new Text();
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
	FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
	String fileName = fileSplit.getPath().getName();
	location.set(fileName);

	String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
	
	while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            output.collect(word, location);
        } 
    }
 } 
