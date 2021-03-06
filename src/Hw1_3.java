import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Hw1_3 {

 public static void main(String[] args) throws Exception {

    JobConf conf = new JobConf(Hw1_3.class);
    conf.setJobName("invertedindex");
        
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
        
    conf.setMapperClass(Hw1_3Mapper.class);
    conf.setReducerClass(Hw1_3Reducer.class);
        
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
        
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
    JobClient.runJob(conf);
 }
        
}
