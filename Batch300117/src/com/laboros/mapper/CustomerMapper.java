package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomerMapper extends Mapper<LongWritable, Text, Text, Text> {

	final IntWritable ONE = new IntWritable(1);

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		
		//key -- 0
		//value  -- 4000001,Kristina,Chung,55,Pilot
		
		final String line=value.toString();
		if(!StringUtils.isEmpty(line))
		{
			final String[] columns=StringUtils.splitPreserveAllTokens(line, ",");
			
				context.write(new Text(columns[0]), new Text("CUSTS\t"+columns[1]));
		}
	};
}
