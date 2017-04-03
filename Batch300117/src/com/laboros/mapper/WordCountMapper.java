package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	final IntWritable ONE = new IntWritable(1);

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		//key -- 0
		//value -- {DEER RIVER RIVER}
		
//		final long lKey=key.get();
		final String iLine=value.toString();
		
		//Validate iLine is not null or empty
		if(!StringUtils.isEmpty(iLine))
		{
			final String[] words=StringUtils.splitPreserveAllTokens(iLine, " ");
			
			final Text outputKey=new Text();
			
			for (String word : words) 
			{
				outputKey.set(word);
			    context.write(outputKey, ONE);	
			}
		}
	};
}
