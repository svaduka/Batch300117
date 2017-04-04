package com.laboros.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {

			String myKey = key.toString().toLowerCase();

			if (myKey.equalsIgnoreCase("hadoop")) {
				return 0;
			}
			if (myKey.equalsIgnoreCase("data")) {
				return 1;
			} else {
				return 2;
			}
		}
}