package com.laboros.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.GrepMapper;
import com.laboros.reducer.GrepReducer;

public class GrepJob extends Configured implements Tool {

	public static void main(String[] args) 
	{
		
		System.out.println("IN MAIN METHOD");
		
		//Step-1 Validation
		if(args.length<2)
		{
			System.out.println("JAVA Usage "+GrepJob.class.getName()+" [configuration] /path/to/local/file /path/hdfs/dest/location");
			return;
		}
		
		//step -2 Loading Configuration
		Configuration conf=new Configuration(Boolean.TRUE);
		try {
			//step-3 : INvoking ToolRunner.run --> It is a generic option parser 
			//parse command line arguments and set to the configuration
			int i=ToolRunner.run(conf, new GrepJob(), args);
			if(i==0)
			{
				System.out.println("SUCCESS");
			}
		} catch (Exception e) {
			System.out.println("FAILURE");
			e.printStackTrace();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception 
	{
		//step -1 : Get the configuration object that was parsed by the toolrunner.run
		Configuration conf=super.getConf();
		//u have to set any parameter to configuration object here
//		final String search_Str=args[2];
//		conf.set("SEARCH_STR", search_Str);
		//Step:2 Job instance is a useful to access the cluster
		Job grepJob=Job.getInstance(conf, GrepJob.class.getName());
	
		//Step: 3 setting classpath for the Mapper program
		grepJob.setJarByClass(GrepJob.class);
		
		//setting input and output
		
		//Step:4 setting input
		final String input=args[0];
		//Convert into URI, because hdfs url always represent in URI
		final Path inputPath= new Path(input);
		TextInputFormat.addInputPath(grepJob, inputPath);
		grepJob.setInputFormatClass(TextInputFormat.class);
		
		//Step:5 setting output
		
		final String output=args[1];
		//Convert into URL, because hdfs url ways represent in URI
		final Path outputPath=new Path(output);
		TextOutputFormat.setOutputPath(grepJob, outputPath);
		grepJob.setOutputFormatClass(TextOutputFormat.class);
		
		//setting mapper and Reducer
		
		//Step-6: Set mapper
		grepJob.setMapperClass(GrepMapper.class);
		//Step-7: SetMapper output key and value classes
		grepJob.setMapOutputKeyClass(Text.class);
		grepJob.setMapOutputValueClass(IntWritable.class);
		
		//step-8 : Set reducer
		grepJob.setReducerClass(GrepReducer.class);
		//Step-9: set reducer output key and value classes
		grepJob.setOutputKeyClass(Text.class);
		grepJob.setOutputValueClass(IntWritable.class);
		//step-10
		grepJob.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
