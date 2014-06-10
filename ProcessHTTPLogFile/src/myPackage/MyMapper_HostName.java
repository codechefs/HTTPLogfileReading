package myPackage;

import java.io.IOException;
import java.util.regex.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

	public class MyMapper_HostName extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		     private final static IntWritable one = new IntWritable(1);
		     private Text word=new Text();

			
			      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			        String line = value.toString();
			        String pattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
					
					// Create a Pattern object
				      Pattern r = Pattern.compile(pattern);
				      
				    // Now create matcher object.
				      Matcher m = r.matcher(line);
					
					if(m.find()){
						String HostName=m.group(1);	
						String browser=m.group(9);
						String statusCode=m.group(6);

						word.set(HostName);
						
			            output.collect(word, one);
			        }
			   }
	}

