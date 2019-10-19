package bigdata.Assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import bigdata.Assignment1.Assignment;
import bigdata.Assignment1.Assignment.Map;
import bigdata.Assignment1.Assignment.Combiner;
import bigdata.Assignment1.Assignment.Reduce;

public class Assignment extends Configured implements Tool {

	   public static void main(String[] args) throws Exception {
		      System.out.println(Arrays.toString(args));
		      int res = ToolRunner.run(new Configuration(), new Assignment(), args);
		      
		      System.exit(res);
	   }
	
	@Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = Job.getInstance(getConf(), "Assignment");
	      job.setJarByClass(Assignment.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);
	      job.setCombinerClass(Combiner.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	      
	      return 0;
	   }
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		  Text word;
		  IntWritable ONE;
		  private HashMap<Text,ArrayList<IntWritable>> hm;
		  private String[] Value;
		  
		  
		  protected void setup(Context context) throws IOException, InterruptedException {
		  
			  hm = new HashMap<Text,ArrayList<IntWritable>>();
		  }
		  
		  @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
			 
			 word = new Text();
			 Value = value.toString().split(";");
	    	 ONE = new IntWritable(Integer.parseInt(Value[5]));
			 word.set(Value[0]);
			 
			 if (hm.get(word) == null) {
				    hm.put(word, new ArrayList<IntWritable>());
				}
			 
			 hm.get(word).add(ONE);
			 
	      }
		  
		  @Override
		  protected void cleanup(Context context)throws IOException, InterruptedException {

			  
			  for (Entry<Text, ArrayList<IntWritable>> ee : hm.entrySet()) {
				  
				    Text K = new Text();
				    K.set(ee.getKey());
				    ArrayList<IntWritable> values = ee.getValue();
				}
			  
			  
			  for(Text k : hm.keySet()) {
				  
				  ArrayList<IntWritable> arr = new ArrayList<IntWritable>();  
				  arr = hm.get(k);
				  
				  for(IntWritable val:arr)
					  context.write(k, val);
					
			  }
			  
		  }
	}
		  

		 public static class Combiner extends Reducer<Text,IntWritable,Text,IntWritable>
		   {
			   
			   int Counter = 0;
			   ArrayList<Integer> list;
			   
			   public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException
			   {
				   int MAX = 0;
				   int AVG = 0;
				   int MIN = 0;
				   int Count = 0;
				   list = new ArrayList<Integer>();
				   
				   System.out.println("KEY ----- "+key);
				   
				   
				   for (IntWritable val : value) {
					   
					   Count++;
					   
					   if(Count == 1)MIN = val.get();
				
					   AVG += val.get();
					   
					   if(MAX < val.get()) MAX = val.get();
					   
					   if(MIN > val.get()) MIN = val.get();
					   
			         }
				   
				   AVG /= Count; 
				   
				   System.out.println("MAX = "+MAX);
				   System.out.println("MIN = "+MIN);
				   System.out.println("AVG = "+AVG);
		           
				   System.out.println("--------------------------------------------- ");
				   
				   list.add(MAX);
				   list.add(MIN);
				   list.add(AVG);
				   
				   for(Integer x : list) {
					   context.write(key, new IntWritable(x));
				   }
				   
			   }
			   
		   }
		 
	   public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		   
		   HashMap<Text,ArrayList<IntWritable>> hm = new HashMap<Text,ArrayList<IntWritable>>();
    	   ArrayList<Integer> arr;
    	   IntWritable MAX;
    	   IntWritable MIN;
    	   IntWritable AVG;
    	   int Counter;
    	   
	      @Override
	      public void reduce(Text key, Iterable<IntWritable> values, Context context)
	              throws IOException, InterruptedException {
	    	  
	    	   MAX = new IntWritable();
	    	   MIN = new IntWritable();
	    	   AVG = new IntWritable();
	    	   arr = new ArrayList<Integer>(3);
	    	   Counter = 0;
	    	   
			   if (hm.get(key) == null) {
					    hm.put(key, new ArrayList<IntWritable>());
			   }
			   
			   for(IntWritable x : values) {
				 hm.get(key).add(x);
				 arr.add(x.get());
			   }

			   
			   for(int x : arr) {
				   
				   if(Counter == 0)MAX = new IntWritable(x);
				   else if(Counter == 1)MIN = new IntWritable(x);
				   else AVG = new IntWritable(x);
				   Counter++;
			   }
		
				String s = String.format("Age = %s, Max Balance = %s, Min Balance = %s, Avg Balance = %s",key,MAX,MIN,AVG);
				context.write(new Text(""), new Text(s));
	    	  
	      }
	   }
	   

}
