import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Error404 {
	
	public static class Error404Mapper extends MapReduceBase implements Mapper<LongWritable,Text,String,String>
	{
		
		  public void map(LongWritable key, Text value, OutputCollector<String,String> output,Reporter reporter)
		      throws IOException {
			  
			  String[] line = value.toString().split("\\s");
			  if(line[8].contains("404")==true)
			  {
				String temp = line[3] + line[4] + " " + line[10];
			    output.collect("404",temp);	  
			  }
		  }
		
	}
	
	/*
	public static class RequestCounterReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>
	{
		
		public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text,IntWritable> output,Reporter reporter)
			      throws IOException {
				  
				  int sum =0;
				  while(values.hasNext())
				  {
					  sum += values.next().get();
				  }
				  output.collect(key, new IntWritable(sum));

			  }	
	}
	*/

  public static void main(String[] args) throws Exception {
	  
    if (args.length != 2) {
      System.err.println("Usage: Error404 <input path> <output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(Error404.class);
    conf.setJobName("Error404");
    
    conf.setNumReduceTasks(0);

    conf.setMapperClass(Error404Mapper.class);
    //conf.setReducerClass(RequestCounterReducer.class);
    
    //conf.setOutputKeyClass(String.class);
    //conf.setOutputValueClass(String.class);
    
    
    //conf.setInputFormat(TextInputFormat.class);
    //conf.setOutputFormat(TextOutputFormat.class);
    
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    JobClient.runJob(conf);

  }
}