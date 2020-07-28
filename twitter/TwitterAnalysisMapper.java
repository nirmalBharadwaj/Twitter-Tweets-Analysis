package com.twitter;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.log4j.Logger;

public class TwitterAnalysisMapper extends Mapper<LongWritable, Text,LongWritable,Text> {
    
	private URI[] files;
	private Map<String,String> AFINN_map;
	private Logger logger = Logger.getLogger(TwitterAnalysisMapper.class);
	
	public void setup(Context context) {
		
		try {
			AFINN_map = new HashMap<String,String>();
			
			URI[] cacheFiles = context.getCacheFiles(); 
			
			
			if(cacheFiles != null && cacheFiles.length >0) {
				String line = ""; 
                FileSystem fs = FileSystem.get(context.getConfiguration()); 
                Path path = new Path(cacheFiles[0].toString()); 
                
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                while ((line = reader.readLine()) != null)  
                { 

                	String []splits = line.split("\t");                 
        			 AFINN_map.put(splits[0], splits[1]);
       			    	
                } 
                reader.close();

			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			 System.out.println("Unable to read the cached filed");
			    System.exit(1);
		}
	}
	public void map(LongWritable keyIn, Text valueIn ,Context context) {
		
		
		String twt;
		LongWritable tweet_id = new LongWritable();
		JSONObject obj=null;
		
		String line  = valueIn.toString();
		JSONParser jsonParser = new JSONParser();
		
		try {
			obj = (JSONObject) jsonParser.parse(line);
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		 
		tweet_id.set(Long.parseLong((String)obj.get("id_str"))); 
		String tweet_text=(String) obj.get("text");
		twt=(String) obj.get("text");
		
		String[] splits = twt.toString().split(" ");
		 
		int sentiment_sum=0;
		 
		for(String word:splits){
		 
		if(AFINN_map.containsKey(word))
		 
		{
		 
		Integer x=new Integer(AFINN_map.get(word));
		 
		sentiment_sum+=x;
		 
		}
		 
		}
		 
		try {
			context.write(tweet_id,new Text(tweet_text+"\t----->\t"+new Text(Integer.toString(sentiment_sum))));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
