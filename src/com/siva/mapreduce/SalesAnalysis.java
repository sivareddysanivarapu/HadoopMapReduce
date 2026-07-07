package com.siva.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// ============================
// Mapper Class
// ============================
public class SalesAnalysis {

    public static class SalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text category = new Text();
        private DoubleWritable sales = new DoubleWritable();

        //D:\Hadoop\MR Practice Data\SalesData 
        
        @Override
        public void map(LongWritable key, // byte offset of the line
        		Text value,//T001,P100,Electronics,500,2,2024-01-15
        		Context context)
                throws IOException, InterruptedException {

            String line = value.toString();           // Convert the line of text to a Java String
            String[] fields = line.split(",");        // Split the CSV line by comma

            // Skip header line (first line of the CSV file)
            if (fields[0].equals("TransactionID")) {
                return;
            }

            // Ensure the line has enough fields
            if (fields.length >= 5) {
                String categoryName = fields[2];                // Category name column
                double price = Double.parseDouble(fields[3]);   // Price column
                int quantity = Integer.parseInt(fields[4]);     // Quantity column
                double totalSales = price * quantity;           // Calculate total = price × quantity

                category.set(categoryName);     // Set the key (category)
                sales.set(totalSales);          // Set the value (total sales)
                context.write(category, sales); // Emit key-value pair to Reducer
            }
        }
    }

    // ============================
    // Reducer Class
    // ============================
    public static class SalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
        	//here why Iterable<DoubleWritable> values, from mapper we only returned DoubleWritable
        	//but before Reducer stage there are merge,shuffle sub stages in reduce phase
        	//these stages will merge all outputs from all the mappers and shuffle will take values as List<> for repeated
        	//we can modify the default beaviour of SHuffle sub stage

            double total = 0.0;

            // Sum all sales values for the same category
            for (DoubleWritable val : values) {
                total += val.get();
            }

            result.set(total);
            context.write(key, result);  // Output: Category → Total Sales
        }
    }

    // ============================
    // Driver (Main) Class
    // ============================
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SalesAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sales Analysis"); // Create job configuration

        job.setJarByClass(SalesAnalysis.class);    // Identify the main class (this one)
        job.setMapperClass(SalesMapper.class);     // Register Mapper
        job.setReducerClass(SalesReducer.class);   // Register Reducer

//        job.setNumReduceTasks(0);this can be used to set number of reducers needed for this job
        //if zero, no reducers , only mappers will need to take care all logic
        //hadoop wont execute reducer phase, directly provides mapper o/p
        //or left to Hadoop’s default (usually 1 reducer)
        
        // Define Mapper output key/value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Define input and output paths in HDFS
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit job and exit based on completion status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

