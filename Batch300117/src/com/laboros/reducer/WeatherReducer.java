package com.laboros.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(
			Text key,
			java.lang.Iterable<Text> values,
			Context context)
			throws java.io.IOException, InterruptedException {
		
		//key -- > 2014
		//values --{20140101	-16.6, 20140102	-17.2, 
		//20140103	-19.3,  20140104	-18.9,  20140105	-17.9}
		
		double maxValue=Double.MIN_VALUE;
		String date=null;

		for(Text value: values)
		  {
			  final String[] tokens=StringUtils.split(value.toString(), "\t");
			  double temp_max=Double.parseDouble(tokens[1]);
		    if( temp_max> maxValue){
			  maxValue = temp_max;
			  date=tokens[0];
			}
		  }
		context.write(key, new Text(date+"\t"+maxValue));
	};
}
