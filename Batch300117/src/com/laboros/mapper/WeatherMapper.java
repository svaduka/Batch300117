package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {
		
		//key -- > 0
		//value -->27516 20140101  2.424 -156.61   71.32   -16.6   -18.7   -17.7   -17.7     0.0     0.00 C   -17.8   -19.4   -18.7    83.8    73.5    80.8 -99.000 -99.000 -99.000 -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0

		final String line=value.toString();
		
		if(!StringUtils.isEmpty(line))
		{
//			final String[] tokens=StringUtils.splitPreserveAllTokens(line, " ");
			final String date=StringUtils.substring(line, 6, 14); //20140101
			final String year=StringUtils.substring(date, 0,4);//2014
			
			final String max_temp=StringUtils.substring(line, 38,45);//-16.6
			
			context.write(new Text(year), new Text(date+"\t"+max_temp));
			
		}
		
		
		
		
	};
}
