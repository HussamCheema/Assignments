package bigdata.Assignment4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
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

import bigdata.Assignment4.Assignment4;
//import bigdata.Assignment4.Assignment4.MyCombiner;
import bigdata.Assignment4.Assignment4.Map;
import bigdata.Assignment4.Assignment4.Reduce;

public class Assignment4 extends Configured implements Tool {

	   public static void main(String[] args) throws Exception {
		      System.out.println(Arrays.toString(args));
		      int res = ToolRunner.run(new Configuration(), new Assignment4(), args);
		      
		      System.exit(res);
	   }
	
	@Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      
	      Job job = Job.getInstance(getConf(), "Assignment4");
	      job.setJarByClass(Assignment4.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);
	      
	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      if (!job.waitForCompletion(true)) {
	    	  System.exit(1);
	    	}
	      
	      // Creating Job2
	      Job job2 = Job.getInstance(getConf(), "Assignment4");
	      
	      job2.addCacheFile(new Path(args[1]+"/part-r-00000").toUri());
	      
	      job2.setJarByClass(Assignment4.class);
	      job2.setOutputKeyClass(Text.class);
	      job2.setOutputValueClass(IntWritable.class);

	      job2.setMapperClass(Map2.class);
	      job2.setReducerClass(Reduce2.class);
	      //job.setCombinerClass(MyCombiner.class);

	      job2.setInputFormatClass(TextInputFormat.class);
	      job2.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job2, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job2, new Path(args[2]));

	      job2.waitForCompletion(true);
	      
	      return 0;
	   }
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		  private String[] Value;
		  private ArrayList<String> Subsets;
		  private HashMap<String,ArrayList<Integer>> hm;
		  private Text word;
		  private int rows = 0;
		  private double SupportThreshold = 0.3;
		  
		  protected void setup(Context context) throws IOException, InterruptedException {
		  
			  hm = new HashMap<String,ArrayList<Integer>>();
			  
		  }// End setup Function
		  
		  @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
			 
			  Value = value.toString().split(",");
			  
			  Subsets = printSubsets(Value);
	          
			  for(String x:Subsets) {

				  if (hm.get(x) == null) {
					    hm.put(x, new ArrayList<Integer>());
					 }
				 
				  hm.get(x).add(1);
			  }
			  
			  rows++;
			  
	      }// End map Function
		  
		  @Override
		  protected void cleanup(Context context)throws IOException, InterruptedException {

			  double localSupport = SupportThreshold * rows;
			  
			  for (Entry<String, ArrayList<Integer>> ee : hm.entrySet()) {

				    ArrayList<Integer> values = ee.getValue();
				    
				    if(values.size() > localSupport)
				    	context.write(new Text(ee.getKey()), new IntWritable(values.size()));
			  	    
				}
			  
		  }// End Cleanup Function 
		  
		  
		  
		   // Customized function to generate subsets
		   public static ArrayList<String> printSubsets(String set[]) 
		    { 
		        int n = set.length; 
		        ArrayList<String> strings = new ArrayList<String>();
		  
		        // Run a loop for printing all 2^n 
		        // subsets one by obe 
		        for (int i = 0; i < (1<<n); i++) 
		        { 
		        	StringBuilder s = new StringBuilder();
		            // Print current subset 
		            for (int j = 0; j < n; j++) { 
		  
		                if ((i & (1 << j)) > 0) {
		                	
		                	s.append(set[j]+"&");
		                }
		            }
		            
		            strings.add(s.toString());

		        } 
		        
		        return strings;
		    } 

		  
	}// END MAP CLASS
		  
	
		 
	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		
	      @Override
	      public void reduce(Text key, Iterable<IntWritable> values, Context context)
	              throws IOException, InterruptedException {
	    	
	    	  context.write(key,new Text(""));
	    	  
	      }// END Reduce Function
	      
	   }// END REDUCER CLASS
	
	public static class Map2 extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		  private ArrayList<String> lines;

		  private String s;
		  private String[] Value;
		  private ArrayList<String[]> list;
		  private double SupportThreshold2 = 0.4;
		  private int rows = 0;

		  private HashMap<String[],Integer> hm;

		  
		  protected void setup(Context context) throws IOException, InterruptedException {
			  
			  lines = new ArrayList<String>();
			  list = new ArrayList<String[]>();

			  hm = new HashMap<String[],Integer>();
			  
			   java.net.URI[] files = context.getCacheFiles();
			   
			   BufferedReader bufferedReader = new BufferedReader(new FileReader(files[0].toString()));
			  
			   while((s = bufferedReader.readLine()) != null) {
				   lines.add(s);
				}
			   
			   for(String x:lines) {
					  Value = x.toString().split("&");
					  list.add(Value);
				}
			  
		  }// End setup Function
		
		  @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
			  
			  
			  for(String[] x:list) {
				  
				  boolean flag = CheckInDatabase(x,value.toString());
				  
				  if(flag) {
					  
				       if(!hm.containsKey(x)){
				        	 
				    	   hm.put(x,1);
				       }
				       else{
				    	   hm.put(x, hm.get(x)+1);
				       }
				  }
				  
			  }
			  
			  rows++;

		  }//End map function
		  
		  @Override
		  protected void cleanup(Context context)throws IOException, InterruptedException {

			  // pm threshold where p is support threshold and m is total transactions
			  double localSupport = SupportThreshold2 * rows;
			  String myString;
			  
			  
			  for (Entry<String[],Integer> ee : hm.entrySet()) {

				    myString = "";
				  
				    for(String x : ee.getKey()) {
				    	myString += x+",";
				    }	  
				    
				    // Removing Space Problem
				    myString = myString.replaceAll("\\s","");
				    
				    int value = ee.getValue();
				    
				    if(value > localSupport)
				    	context.write(new Text(myString), new IntWritable(value));
				}
			  
		  }// End Cleanup Function 
		  
		  
		  // Customized function to check, whether our rules exist in the database or not
		  // If they exist then return true else false
		  public boolean CheckInDatabase(String[] CheckList, String Value) {

			  int SIZE = CheckList.length - 1;
			  int counter = 0;
			  
			  for(String a: CheckList) {
				  
				  if(Value.contains(a)) {
					  counter++;
				  }
			  }
			  
			  if(counter >= SIZE) {
				  return true;
			  }
			  else return false;
			  
		  }//End CheckInDatabase Function
		
	}// END Mapper2 Class
	
	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
	
	      @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	    	  
	    	  context.write(key, new Text(""));
	    	  
	      }
		
	}// END Reducer2 Class
	   

}// END Assignment4 CLASS
