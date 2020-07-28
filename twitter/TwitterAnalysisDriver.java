package com.twitter;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import java.net.URI; 

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TwitterAnalysisDriver {
	
	public static void main(String[] args) throws IOException, InterruptedException  {
	 
		 Configuration conf = new Configuration();
		
			
		 try {
			 Job job = Job.getInstance(conf,"twitter analysis");
			 job.setJarByClass(TwitterAnalysisDriver.class);
			 job.setMapperClass(TwitterAnalysisMapper.class);
			 job.setNumReduceTasks(0);
			 
			 job.setMapOutputKeyClass(LongWritable.class);
			 job.setMapOutputValueClass(Text.class);
			 job.setOutputKeyClass(LongWritable.class);
			 job.setOutputValueClass(Text.class);
			 
			 
			 // Input and output format
			 job.setInputFormatClass(TextInputFormat.class);
			 job.setOutputFormatClass(TextOutputFormat.class);
			 
			 
			 job.addCacheFile(new URI("hdfs://localhost:9000/twitter-data/AFINN"));
			 
			// set input and output path 
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
			 job.waitForCompletion(true);
			 
			 
		 }
		 catch(Exception e) {
			 System.out.print("Failure: "+e.getMessage());
		 }
			
			
		 
		
	}

}
