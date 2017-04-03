package com.laboros.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeIdentifyReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	protected void reduce(Text key, java.lang.Iterable<IntWritable> values,
			Context context) throws java.io.IOException, InterruptedException 
	{};
}
