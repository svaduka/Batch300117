package com.laboros.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.DeIdentifyMapper;

public class DeIdentifyJob extends Configured implements Tool {

	public static void main(String[] args) 
	{
		
		System.out.println("IN MAIN METHOD");
		
		//Step-1 Validation
		if(args.length<2)
		{
			System.out.println("JAVA Usage "+DeIdentifyJob.class.getName()+" [configuration] /path/to/hdfs/file /path/hdfs/dest/location");
			return;
		}
		
		//step -2 Loading Configuration
		Configuration conf=new Configuration(Boolean.TRUE);
		try {
			//step-3 : INvoking ToolRunner.run --> It is a generic option parser 
			//parse command line arguments and set to the configuration
			int i=ToolRunner.run(conf, new DeIdentifyJob(), args);
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
		
		//Step:2 Job instance is a useful to access the cluster
		Job deIdentifyJob=Job.getInstance(conf, DeIdentifyJob.class.getName());
	
		//Step: 3 setting classpath for the Mapper program
		deIdentifyJob.setJarByClass(DeIdentifyJob.class);
		
		//setting input and output
		
		//Step:4 setting input
		final String input=args[0];
		//Convert into URI, because hdfs url always represent in URI
		final Path inputPath= new Path(input);
		TextInputFormat.addInputPath(deIdentifyJob, inputPath);
		deIdentifyJob.setInputFormatClass(TextInputFormat.class);
		
		//Step:5 setting output
		
		final String output=args[1];
		//Convert into URL, because hdfs url ways represent in URI
		final Path outputPath=new Path(output);
		TextOutputFormat.setOutputPath(deIdentifyJob, outputPath);
		deIdentifyJob.setOutputFormatClass(TextOutputFormat.class);
		
		//setting mapper and Reducer
		
		//Step-6: Set mapper
		deIdentifyJob.setMapperClass(DeIdentifyMapper.class);
		//Step-7: SetMapper output key and value classes
		deIdentifyJob.setMapOutputKeyClass(Text.class);
		deIdentifyJob.setMapOutputValueClass(NullWritable.class);
		
		//step-8 : Set reducer
//		deIdentifyJob.setReducerClass(DeIdentifyReducer.class);
		//Step-9: set reducer output key and value classes
//		deIdentifyJob.setOutputKeyClass(Text.class);
//		deIdentifyJob.setOutputValueClass(IntWritable.class);
		//step-10
		deIdentifyJob.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
