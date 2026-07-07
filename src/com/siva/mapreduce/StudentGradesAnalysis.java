package com.siva.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentGradesAnalysis {
	

	public static class GradesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	    
	    private Text subject = new Text();
	    private DoubleWritable marks = new DoubleWritable();
	    
	    @Override
	    public void map(LongWritable key, Text value, Context context) 
	            throws IOException, InterruptedException {
	        
	        String line = value.toString();
	        String[] fields = line.split(",");
	        
	        // Skip header
	        if (fields[0].equals("StudentID")) {
	            return;
	        }
	        
	        if (fields.length >= 4) {
	            String subjectName = fields[2];
	            double mark = Double.parseDouble(fields[3]);
	            
	            subject.set(subjectName);
	            marks.set(mark);
	            context.write(subject, marks);
	        }
	    }
	}


	public static class GradesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	    
	    private DoubleWritable result = new DoubleWritable();
	    
	    @Override
	    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
	            throws IOException, InterruptedException {
	        
	        double sum = 0.0;
	        int count = 0;
	        
	        for (DoubleWritable val : values) {
	            sum += val.get();
	            count++;
	        }
	        
	        double average = sum / count;
	        result.set(average);
	        context.write(key, result);
	    }
	}

	public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: GradesDriver <input path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Student Grades Analysis");
        
        job.setJarByClass(StudentGradesAnalysis.class);
        job.setMapperClass(GradesMapper.class);
        job.setReducerClass(GradesReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
