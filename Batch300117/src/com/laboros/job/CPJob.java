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
import com.laboros.partitioner.MyPartitioner;

import com.laboros.mapper.WordCountMapper;
import com.laboros.reducer.WordCountReducer;

public class CPJob extends Configured implements Tool {

	public static void main(String[] args) 
	{
		
		System.out.println("IN MAIN METHOD");
		
		//Step-1 Validation
		if(args.length<2)
		{
			System.out.println("JAVA Usage "+CPJob.class.getName()+" [configuration] /path/to/hdfs/file /path/hdfs/dest/location");
			return;
		}
		
		//step -2 Loading Configuration
		Configuration conf=new Configuration(Boolean.TRUE);
		try {
			//step-3 : INvoking ToolRunner.run --> It is a generic option parser 
			//parse command line arguments and set to the configuration
			int i=ToolRunner.run(conf, new CPJob(), args);
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
		Job cpJob=Job.getInstance(conf, CPJob.class.getName());
	
		//Step: 3 setting classpath for the Mapper program
		cpJob.setJarByClass(CPJob.class);
		
		//setting input and output
		
		//Step:4 setting input
		final String input=args[0];
		//Convert into URI, because hdfs url always represent in URI
		final Path inputPath= new Path(input);
		TextInputFormat.addInputPath(cpJob, inputPath);
		cpJob.setInputFormatClass(TextInputFormat.class);
		
		//Step:5 setting output
		
		final String output=args[1];
		//Convert into URL, because hdfs url ways represent in URI
		final Path outputPath=new Path(output);
		TextOutputFormat.setOutputPath(cpJob, outputPath);
		cpJob.setOutputFormatClass(TextOutputFormat.class);
		
		//setting mapper and Reducer
		
		//Step-6: Set mapper
		cpJob.setMapperClass(WordCountMapper.class);
		//Step-7: SetMapper output key and value classes
		cpJob.setMapOutputKeyClass(Text.class);
		cpJob.setMapOutputValueClass(IntWritable.class);
		cpJob.setCombinerClass(WordCountReducer.class);
		cpJob.setNumReduceTasks(3);
		cpJob.setPartitionerClass(MyPartitioner.class);
		
		//step-8 : Set reducer
		cpJob.setReducerClass(WordCountReducer.class);
		//Step-9: set reducer output key and value classes
		cpJob.setOutputKeyClass(Text.class);
		cpJob.setOutputValueClass(IntWritable.class);
		//step-10
		cpJob.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
