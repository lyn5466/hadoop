import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
        
public class Tweeter2 {
        
 public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
    private final static Text location = new Text();
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
	
            output.collect(key, value);
    }
 } 
        
 public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {

    public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
	while (values.hasNext()) {
	    String theText = values.next().toString();
	    output.collect(key, new Text(theText.toString()));
        }
    }
 }
        
 public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Tweeter2.class);
    conf.setJobName("wordcount");
        
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);
        
    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
        
    conf.setInputFormat(PatternInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
        
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
    JobClient.runJob(conf);
 }
        
}
