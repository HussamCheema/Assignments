package bigdata.Assignment1;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Problem2 extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new Problem2(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = Job.getInstance(getConf(), "Problem2");
		job.setJarByClass(Problem2.class);
		job.setNumReduceTasks(12);

		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);

		job.setPartitionerClass(JobPartitioner.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

			Text Key = new Text();
			String[] rows = value.toString().split(";");
			
			Text data = new Text("Age = "+ String.valueOf(rows[0])+" marital = "+ String.valueOf(rows[2])+" Education ="+ String.valueOf(rows[3])+" Default = "+String.valueOf(rows[4])+
					" Balance ="+ String.valueOf(rows[5])+" Housing = "+String.valueOf(rows[6])+" Loan = "+ String.valueOf(rows[7]));
			
			// Using regular expression to remove quotes
			Key.set(rows[1].replaceAll("^\"|\"$", ""));
			
			context.write(Key, data);
			
		}
	}
	public static class JobPartitioner extends Partitioner<Text,Text>{
		
		@Override
		public int getPartition(Text key, Text value, int numPartitions)
		{

			if(numPartitions==0) {
				System.out.println("0");
				return 0;
			}
			
			if(key.equals(new Text("admin.")))
				return 0;
			
			if(key.equals(new Text("unknown")))
				return 1;
			
			if(key.equals(new Text("unemployed")))
				return 2;
			
			if(key.equals(new Text("management")))
				return 3;
			
			if(key.equals(new Text("housemaid")))
				return 4;
			
			if(key.equals(new Text("entrepreneur")))
				return 5;
			
			if(key.equals(new Text("student")))
				return 6;
			
			if(key.equals(new Text("blue-collar")))
				return 7;
			
			if(key.equals(new Text("self-employed")))
				return 8;
			
			if(key.equals(new Text("retired")))
				return 9;
			
			if(key.equals(new Text("technician")))
				return 10;
			
			if(key.equals(new Text("services")))
				return 11;
			
			else
				return 0;

		}
	}

	public static class Combiner extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException
		{

			for (Text val : value) {
				context.write(key,new Text(val.toString()));
			}
			
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

			for (Text val : values) {
				context.write(key,new Text(val.toString()));
			}
			
		}
	}
}