import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
        
public class Tweeter {
        
 public static class Map extends MapReduceBase implements Mapper<Text, IdTime, Text, IdTime> {
    //private IdTime myIdTime = new IdTime();
    //private Text time = new Text();
    private Text word = new Text();
        
    public void map(Text key, IdTime value, OutputCollector<Text, IdTime> output, Reporter reporter) throws IOException {
	    String line = key.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
	
	    while (tokenizer.hasMoreTokens()) {
            	word.set(tokenizer.nextToken());
            	output.collect(word, value);
            } 
            //output.collect(key, value);
    }
 } 
        
 public static class Reduce extends MapReduceBase implements Reducer<Text, IdTime, Text, Text> {

    public void reduce(Text key, Iterator<IdTime> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	StringBuilder theResult = new StringBuilder();
	while (values.hasNext()) {
	    String theText = values.next().toString();
	    theResult.append(theText + " ");
        }
	output.collect(key, new Text(theResult.toString()));
    }
 }
        
 public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Tweeter.class);
    conf.setJobName("wordcount");
        
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapOutputValueClass(IdTime.class);
        
    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setNumMapTasks(Integer.valueOf(args[2]));
    conf.setNumReduceTasks(Integer.valueOf(args[3]));
        
    conf.setInputFormat(ObjectPositionInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
        
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
    JobClient.runJob(conf);
 }
        
}
