package com.laboros;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.util.HDFSUtil;

public class HDFSService extends Configured implements Tool {
	
	public static void main(String[] args) {
		
		System.out.println("IN MAIN METHOD");
		
		//Step-1 Validation
		if(args.length<2)
		{
			System.out.println("JAVA Usage "+HDFSService.class.getName()+" [configuration] /path/to/local/file /path/hdfs/dest/location");
			return;
		}
		
		//step -2 Loading Configuration
		Configuration conf=new Configuration(Boolean.TRUE);
		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		try {
			//step-3 : INvoking ToolRunner.run --> It is a generic option parser 
			//parse command line arguments and set to the configuration
			int i=ToolRunner.run(conf, new HDFSService(), args);
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
	public int run(String[] args) throws Exception {
		
		Configuration conf=super.getConf();
		final String input=args[0];
		final String output=args[1];
		
		boolean isSuccess=HDFSUtil.copyFromLocal(conf, input, output);

		return isSuccess?0:1;
		
	}
}
