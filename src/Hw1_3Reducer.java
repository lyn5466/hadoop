import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Hw1_3Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

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
