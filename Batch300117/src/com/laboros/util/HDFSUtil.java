package com.laboros.util;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSUtil {
	
	public static boolean copyFromLocal(Configuration conf, final String input,final String output) throws Exception {
		
		//CopyFrom local method logic will go here
		//step-2 get filesystem object
	
		FileSystem hdfs=FileSystem.get(conf);
		
		
		final String fileName=getFileName(input); //WordCount.txt
		
		//Step -4 create metadata : (Empty File)hdfs destination
		
		final String outputFileName=output+System.getProperty("file.separator")+fileName;
		
		System.out.println("OutFileName:"+outputFileName);
	
		//Convert to URI
		Path hdfsDestPathWithFileName=new Path(outputFileName);
	
		//Step-5 : Invoking NameNode using hdfs fileSytem
		FSDataOutputStream fsDos=hdfs.create(hdfsDestPathWithFileName);
		
		//Step-6: Read input
		InputStream is = new FileInputStream(input);

		//Step-7: ADding data
		readOrWriteFile(is, fsDos, conf);
		
		return Boolean.TRUE;
	}

	
	
	public static boolean copyToLocal(Configuration conf, final String hdfsInput,final String edgeNodeLocalFile) throws Exception {
		
		//CopyFrom local method logic will go here
		//step-2 get filesystem object
	
		FileSystem hdfs=FileSystem.get(conf);
		
		
//		final String fileName=getFileName(hdfsInput); //user/edureka/WordCount.txt
		final Path hdfsInputPath=new Path(hdfsInput);
		
		//Step-5 : Invoking NameNode using hdfs fileSytem
		FSDataInputStream fsDis=hdfs.open(hdfsInputPath);
		
		//Step-6: Read input
		OutputStream os = new FileOutputStream(edgeNodeLocalFile);

		//Step-7: ADding data
		readOrWriteFile(fsDis, os, conf);
		
		return Boolean.TRUE;
	}

	final static String getFileName(final String inputFileWithLoc)
	{
		String fileName=inputFileWithLoc;
		final String separator=System.getProperty("file.separator");
		final int idx=StringUtils.lastIndexOf(inputFileWithLoc, separator);
		if(idx!=-1)
		{
			fileName=StringUtils.substring(inputFileWithLoc, idx);
		}
		
		return fileName;
	}
	
	public static boolean readOrWriteFile(final InputStream is, final OutputStream os, final Configuration conf) throws IOException
	{
		IOUtils.copyBytes(is, os, conf, Boolean.TRUE);
		return Boolean.TRUE;
	}

}
