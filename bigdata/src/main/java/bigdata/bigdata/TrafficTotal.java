package bigdata.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 *  It's used to get a total statistics for traffic violation every day
 * 
 */
public class TrafficTotal {
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(Object obj, Text line, Context context) 
		    throws IOException, InterruptedException {
			String[] words = line.toString().split(",");
 			if (words.length == 0 || words[0] == null || words[0].charAt(0) > '9' || 
 					 words[0].charAt(0) < '0')
 				return ;
 			
 			context.write(new Text(words[0]), new IntWritable(1));
		}
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private static IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		    throws IOException, InterruptedException {
		    int sum = 0;
		    for (IntWritable element : values) {
		    	sum += element.get();
		    }
		    
		    result.set(sum);
		    context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2){
			System.out.println("Usage: TrafficTotal <input> <output>");
			System.exit(1);
		}
		
 		Job job = Job.getInstance(conf, "traffic statistics");
	    job.setJarByClass(TrafficTotal.class);
	    
	    job.setMapperClass(MyMapper.class);
	    job.setReducerClass(MyReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
