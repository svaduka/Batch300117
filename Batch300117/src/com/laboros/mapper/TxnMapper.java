package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TxnMapper extends Mapper<LongWritable, Text, Text, Text> {

	final IntWritable ONE = new IntWritable(1);

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		//key -- 0
		
		//value -- 00000000,06-26-2011,4000001,040.33,
		//Exercise & Fitness,Cardio Machine Accessories,Clarksville,Tennessee,credit
		
		final String line=value.toString();
		if(StringUtils.isNotEmpty(line))
		{
			final String[] columns=StringUtils.splitPreserveAllTokens(line, ",");
			context.write(new Text(columns[2]), new Text("TXNS\t"+columns[3]));
		}
	};
}