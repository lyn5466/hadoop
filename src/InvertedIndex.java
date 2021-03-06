import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
        
public class InvertedIndex {
        
 public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
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
        
 public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

	java.util.Map<String, Integer> myMap = new java.util.HashMap<String, Integer>();
	        
	while (values.hasNext()) {
	    String theText = values.next().toString();
            if (myMap.containsKey(theText)) {	    	
		myMap.put(theText, myMap.get(theText) + 1);
	    }
	    else myMap.put(theText, 1);
        }
	StringBuilder theResult = new StringBuilder();
	for (java.util.Map.Entry<String, Integer> myEntry : myMap.entrySet()) 
		theResult.append(myEntry.getKey() + ":" + myEntry.getValue() + " ");
        output.collect(key, new Text(theResult.toString()));

    }
 }
        
 public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(WordCount.class);
    conf.setJobName("wordcount");
        
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
        
    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
        
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
        
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
    JobClient.runJob(conf);
 }
        
}
