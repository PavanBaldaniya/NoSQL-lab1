import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class RequestCounter {
	
	public static class RequestCounterMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>
	{
		
		  public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output,Reporter reporter)
		      throws IOException {
			    
			  String[] line = value.toString().split("\\s");
			  String month = line[3].substring(4,7);
			  String year = line[3].substring(8,12);
			  String mykey = month + " " + year ;
			  
			  Text pass = new Text();
			  pass.set(mykey);
			  IntWritable pass2 = new IntWritable();
			  pass2.set(Integer.parseInt(line[8]));
			  output.collect(pass, pass2);
			
		  }
		
	}
	
	
	public static class RequestCounterReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>
	{
		
		public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text,IntWritable> output,Reporter reporter)
			      throws IOException {
				  
				  int sum=0;
				  float bytesum =0;
				  while(values.hasNext()==true)
				  {
					  sum+=1;
					  bytesum += values.next().get();
				  }
				  bytesum = bytesum/1048576;
				  String pass = key.toString() + " " + Float.toString(bytesum);
				  key.set(pass);
				  output.collect(key, new IntWritable(sum));

			  }	
	}
	

  public static void main(String[] args) throws Exception {
	  
    if (args.length != 2) {
      System.err.println("Usage: RequestCounter <input path> <output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(RequestCounter.class);
    conf.setJobName("RequestCounter");
    

    conf.setMapperClass(RequestCounterMapper.class);
    conf.setReducerClass(RequestCounterReducer.class);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    
    
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    JobClient.runJob(conf);

  }
}