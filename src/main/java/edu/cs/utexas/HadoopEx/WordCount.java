package edu.cs.utexas.HadoopEx;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();

			Job job = new Job(conf, "WordCount");
			job.setJarByClass(WordCount.class);

			// specify a Mapper
			job.setMapperClass(Task1Mapper.class);

			// specify a Reducer
			job.setReducerClass(Task1Reducer.class);

			// specify output types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setOutputFormatClass(TextOutputFormat.class);

			// return (job.waitForCompletion(true) ? 0 : 1);
			if (!job.waitForCompletion(true)) {
				return 1;
			}
			
			conf = new Configuration();

			// Initialize parameters
			double m = 0.0; // Initial slope
			double b = 0.0; // Initial intercept
			double learningRate = 0.001; // Initial learning rate
			int maxIterations = 100; // Number of iterations
			int NUM_FEATURES = 5;

			double[] parameters = new double[NUM_FEATURES];
        	for(int i = 0; i < NUM_FEATURES; i++) {
            	parameters[i] = 0.1; // Initial parameter values
        	}
	
			conf.set("learningRate", String.valueOf(learningRate));
	
			for (int i = 0; i < maxIterations; i++) {
				// Set current parameters in configuration
				conf.set("m", String.valueOf(m));
				conf.set("b", String.valueOf(b));
	
				job = Job.getInstance(conf, "Gradient Descent Iteration " + i);
				job.setJarByClass(WordCount.class);
	
				job.setMapperClass(Task2Mapper.class);
				job.setReducerClass(Task2Reducer.class);
	
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);
	
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[2] + "/iteration" + i));
	
				// Run the MapReduce job
				if (!job.waitForCompletion(false)) {
					System.err.println("Job failed at iteration " + i);
					System.exit(1);
				}
	
				// Retrieve gradients from counters
				double dm = job.getCounters().findCounter("Gradient", "dm").getValue() / 1e6; // Scale back by 1e6
				double db = job.getCounters().findCounter("Gradient", "db").getValue() / 1e6; // Scale back by 1e6
	
				dm /= job.getCounters().findCounter("Gradient", "c").getValue();
				db /= job.getCounters().findCounter("Gradient", "c").getValue();
				// Update parameters using gradients
				m -= learningRate * dm;
				b -= learningRate * db;
	
				// Print cost and parameters for debugging
				System.out.println("Iteration " + i + ": m=" + m + ", b=" + b);
			}
	
			System.out.println("Final model: m=" + m + ", b=" + b);
	
			if (!job.waitForCompletion(true)) {
				return 1;
			}

			m = 0.1;
			b = 0.1;
			double m2 = 0.1;
			double m3 = 0.1;
			double m4 = 0.1;

			conf = new Configuration();
			conf.set("learningRate", String.valueOf(learningRate));
			for (int i = 0; i < maxIterations; i++) {
				// Set current parameters in configuration
				conf.set("m", String.valueOf(m));
				conf.set("m2", String.valueOf(m2));
				conf.set("m3", String.valueOf(m3));
				conf.set("m4", String.valueOf(m4));
				conf.set("b", String.valueOf(b));
	
				job = Job.getInstance(conf, "Multiple Gradient Descent Iteration " + i);
				job.setJarByClass(WordCount.class);
	
				job.setMapperClass(Task3Mapper.class);
				job.setReducerClass(Task3Reducer.class);
	
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleArrayWritable.class);
	
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[3] + "/iteration" + i));
				if (!job.waitForCompletion(true)) {  
					System.err.println("Task3 failed at iteration " + i);
					return 1;
				}

				double dm = job.getCounters().findCounter("Gradient", "m").getValue() / 1e6; // Scale back by 1e6
				double db = job.getCounters().findCounter("Gradient", "b").getValue() / 1e6; // Scale back by 1e6
				double dm2 = job.getCounters().findCounter("Gradient", "m2").getValue() / 1e6; // Scale back by 1e6
				double dm3 = job.getCounters().findCounter("Gradient", "m3").getValue() / 1e6; // Scale back by 1e6
				double dm4 = job.getCounters().findCounter("Gradient", "m4").getValue() / 1e6; // Scale back by 1e6

				double c = job.getCounters().findCounter("Gradient", "c").getValue();


				dm /= c;
				db /= c;
				dm2 /= c;
				dm3 /= c;
				dm4 /= c;

				m -= learningRate * dm;
				b -= learningRate * db;
				m2 -= learningRate * dm2;
				m3 -= learningRate * dm3;
				m4 -= learningRate * dm4;


			}
			

			return (job.waitForCompletion(true) ? 0 : 1);


		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}




	}
}
