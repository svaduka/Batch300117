package com.laboros.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoinReducer extends
		Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, java.lang.Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException 
	{
		//key -- 4000001
		//values -- {TXNS	098.81,CUSTS	Kristina,TXNS	040.33}
		
		double sum=0;
		int count=0;
		String name=null;
		
		//logic goes here
		
		for (Text input : values) {
			final String strInput=input.toString();
			final String tokens[]=StringUtils.splitPreserveAllTokens(strInput, "\t");
			
			if(StringUtils.equalsIgnoreCase(tokens[0], "TXNS"))
			{
				count++;
				sum+=Double.parseDouble(tokens[1]);
			}else if(StringUtils.equalsIgnoreCase(tokens[0], "CUSTS"))
			{
				name=tokens[1];
			}
		}
		
		
		context.write(new Text(name), new Text(sum+"\t"+count));
		
	};
}
