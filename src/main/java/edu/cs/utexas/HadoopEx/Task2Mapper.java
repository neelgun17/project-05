package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;


class Task2Mapper extends Mapper<Object, Text, Text, DoubleWritable> {
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        // Parse input line into x and y
        String[] tokens = value.toString().split(",");
        double x = Double.parseDouble(tokens[5]);
        double y = Double.parseDouble(tokens[11]);

        // Current parameters m and b (read from configuration or global storage)
        double m = Double.parseDouble(context.getConfiguration().get("m", "0.0"));
        double b = Double.parseDouble(context.getConfiguration().get("b", "0.0"));
        // System.out.println("M: " + m  + "  B: " + b);
        // Compute predictions and gradient components
        double prediction = m * x + b;
        double error = y - prediction;

        // Emit partial gradients
        context.write(new Text("dm"), new DoubleWritable(-2 * x * error));
        context.write(new Text("db"), new DoubleWritable(-2 * error));
    }
}
