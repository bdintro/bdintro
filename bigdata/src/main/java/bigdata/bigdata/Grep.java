package bigdata.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * It's used to print warning information, including line number and 
 * line information 
 */
public class Grep {
	public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable linenumber, Text line, Context context) 
		       throws IOException, InterruptedException {
			String pattern = context.getConfiguration().get("grep");
			
			String linecontent = line.toString();
			if (linecontent.indexOf(pattern) == -1) {
				return ;
			} 
			
			context.write(linenumber, line);
		}
	}
	
	public static class MyReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable linenumber, Iterable<Text> line,  Context context) 
		      throws IOException, InterruptedException {
			for (Text element : line) {
				context.write(linenumber, element);
			}
		}
	}
	
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		
		if (args.length != 3) {
			System.out.println("Usage: Grep <pattern> <input> <output>");
			System.exit(1);
		}
		conf.set("grep", args[0]);
		
 		Job job = Job.getInstance(conf, "shop count");
	    job.setJarByClass(Grep.class);
	    
	    job.setMapperClass(MyMapper.class);
	    job.setReducerClass(MyReducer.class);
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
 }
