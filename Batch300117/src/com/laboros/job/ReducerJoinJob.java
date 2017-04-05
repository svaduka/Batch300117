package com.laboros.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.CustomerMapper;
import com.laboros.mapper.TxnMapper;
import com.laboros.reducer.ReducerJoinReducer;

public class ReducerJoinJob extends Configured implements Tool {

	public static void main(String[] args) 
	{
		
		System.out.println("IN MAIN METHOD");
		
		//Step-1 Validation
		if(args.length<3)
		{
			System.out.println("JAVA Usage "+ReducerJoinJob.class.getName()+" [configuration] /path/to/hdfs/file/custs /path/to/hdfs/file/txns /path/hdfs/dest/location");
			return;
		}
		
		//step -2 Loading Configuration
		Configuration conf=new Configuration(Boolean.TRUE);
		try {
			//step-3 : INvoking ToolRunner.run --> It is a generic option parser 
			//parse command line arguments and set to the configuration
			int i=ToolRunner.run(conf, new ReducerJoinJob(), args);
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
		Job rjJob=Job.getInstance(conf, ReducerJoinJob.class.getName());
	
		//Step: 3 setting classpath for the Mapper program
		rjJob.setJarByClass(ReducerJoinJob.class);
		
		//setting input and output
		
		//Step:4 setting multiple input
		
		//step: 4a : Setting customer Input
		final String custInput=args[0];
		//Convert into URI, because hdfs url always represent in URI
		final Path custInputPath= new Path(custInput);
		
		MultipleInputs.addInputPath(rjJob, custInputPath, TextInputFormat.class, CustomerMapper.class);
		
		//step: 4a : Setting customer Input
		final String txnInput=args[1];
		//Convert into URI, because hdfs url always represent in URI
		final Path txnInputPath= new Path(txnInput);
		MultipleInputs.addInputPath(rjJob, txnInputPath, TextInputFormat.class, TxnMapper.class);

		//Step:5 setting output
		
		final String output=args[2];
		//Convert into URL, because hdfs url ways represent in URI
		final Path outputPath=new Path(output);
		TextOutputFormat.setOutputPath(rjJob, outputPath);
		rjJob.setOutputFormatClass(TextOutputFormat.class);
		
		//step-8 : Set reducer
		rjJob.setReducerClass(ReducerJoinReducer.class);
		//Step-9: set reducer output key and value classes
		rjJob.setOutputKeyClass(Text.class);
		rjJob.setOutputValueClass(Text.class);
		//step-10
		rjJob.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
