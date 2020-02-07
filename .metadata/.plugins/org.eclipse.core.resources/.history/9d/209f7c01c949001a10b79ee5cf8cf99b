import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
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

public class ImageCount {
	
	public static class ImageCounterMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>
	{
		
		private Text somekey = new Text();
		  
		  public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output,Reporter reporter)
		      throws IOException {
			  
			  String line = value.toString();
				  
			  StringTokenizer tokenizer = new StringTokenizer(line);
			  while(tokenizer.hasMoreTokens())
			  {
				  String word = tokenizer.nextToken();
				  if(word.contains(".png")==true)
				  {
					  somekey.set("png");
					  output.collect(somekey,new IntWritable(1));
				  }
				  
				  else if(word.contains(".jpg")==true)
				  {
					  somekey.set("jpg");
					  output.collect(somekey, new IntWritable(1));
				  }
				
				  else if(word.contains(".jpeg")==true)
				  {
					  somekey.set("jpeg");
					  output.collect(somekey, new IntWritable(1));
				  }
				  
				  else if(word.contains(".bmp")==true)
				  {
					  somekey.set("bmp");
					  output.collect(somekey, new IntWritable(1));
				  }
				  
				  else if(word.contains(".tif")==true)
				  {
					  somekey.set("tif");
					  output.collect(somekey, new IntWritable(1));
				  }
				  else if(word.contains(".gif")==true)
				  {
					  somekey.set("gif");
					  output.collect(somekey, new IntWritable(1));
				  }
			  }
			  
		  }
			  
		
	}
	
	public static class ImageCounterReduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>
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
	

  public static void main(String[] args) throws Exception {
	  
    if (args.length != 2) {
      System.err.println("Usage: ImageCount <input path> <output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(ImageCount.class);
    conf.setJobName("ImageCount");

    conf.setMapperClass(ImageCounterMapper.class);
    conf.setReducerClass(ImageCounterReduce.class);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    JobClient.runJob(conf);

  }
}