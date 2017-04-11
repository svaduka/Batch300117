
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class MyDC {
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
        
		
		private Map<String, String> abMap = new HashMap<String, String>();
		
		//abMap
//		up	Uttar_Pradesh
//		ma	Maharashtra
//		bi	Bihar
//		wb	WestBengal
				private Text outputKey = new Text();
				private Text outputValue = new Text();
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			
			for (Path p : files) {
				if (p.getName().equals("abc.dat")) {
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine(); //up	Uttar_Pradesh
					while(line != null) {
						String[] tokens = line.split("\t");
						//tokens[0] = up
						//tokens[1] =Uttar_Pradesh
						String ab = tokens[0]; //up
						String state = tokens[1]; //Uttar_Pradesh
						abMap.put(ab, state);
						line = reader.readLine();
					}
				}
			}
			if (abMap.isEmpty()) {
				throw new IOException("Unable to load Abbrevation data.");
			}
		}

		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	
        	
        	String row = value.toString(); //up	199654321
        	String[] tokens = row.split("\t"); 
        	String inab = tokens[0]; //up
        	String state = abMap.get(inab); // Uttar_pradesh
        	outputKey.set(state);
        	outputValue.set(row);
      	  	context.write(outputKey,outputValue);//Uttar_pradesh up 1232423
        }  
}
	
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
    Job job = new Job();
    job.setJarByClass(MyDC.class);
    job.setJobName("DCTest");
    job.setNumReduceTasks(0);
    
    try{
    DistributedCache.addCacheFile(new URI("/user/edureka/ADMR/abc.dat"), job.getConfiguration());
    }catch(Exception e){
    	System.out.println(e);
    }
    
    job.setMapperClass(MyMapper.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
    
    
  }
}