package com.siva.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WebLogAnalysis {
	
	
	public static class LogAnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	    
	    private Text page = new Text();
	    private final static IntWritable one = new IntWritable(1);
	    
	    @Override
	    public void map(LongWritable key, Text value, Context context) 
	            throws IOException, InterruptedException {
	        
	        String line = value.toString();
	        String[] fields = line.split(",");
	        
	        // Skip header
	        if (fields[0].equals("IP_Address")) {
	            return;
	        }
	        
	        if (fields.length >= 3) {
	            String requestedPage = fields[2];
	            page.set(requestedPage);
	            context.write(page, one);
	        }
	    }
	}

	public static class LogAnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	    
	    private IntWritable result = new IntWritable();
	    
	    @Override
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	            throws IOException, InterruptedException {
	        
	        int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        result.set(sum);
	        context.write(key, result);
	    }
	}

	    
	    public static void main(String[] args) throws Exception {
	        if (args.length != 2) {
	            System.err.println("Usage: LogAnalysisDriver <input path> <output path>");
	            System.exit(-1);
	        }
	        
	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "Web Log Analysis");
	        
	        job.setJarByClass(WebLogAnalysis.class);
	        job.setMapperClass(LogAnalysisMapper.class);
	        job.setReducerClass(LogAnalysisReducer.class);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
}
