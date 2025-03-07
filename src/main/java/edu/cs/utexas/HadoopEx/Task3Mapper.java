package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import edu.cs.utexas.HadoopEx.DoubleArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

class Task3Mapper extends Mapper<Object, Text, Text, DoubleArrayWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Parse input line into x and y
        String[] tokens = value.toString().split(",");
        double x = Double.parseDouble(tokens[4]);
        double x2 = Double.parseDouble(tokens[5]);
        double x3 = Double.parseDouble(tokens[11]);
        double x4 = Double.parseDouble(tokens[15]);
        double y = Double.parseDouble(tokens[16]);

        // Current parameters m and b (read from configuration or global storage)
        double m = Double.parseDouble(context.getConfiguration().get("m", "0.0"));
        double m2 = Double.parseDouble(context.getConfiguration().get("m2", "0.0"));
        double m3 = Double.parseDouble(context.getConfiguration().get("m3", "0.0"));
        double m4 = Double.parseDouble(context.getConfiguration().get("m4", "0.0"));
        double b = Double.parseDouble(context.getConfiguration().get("b", "0.0"));
        double[] parameters = { b, m, m2, m3, m4};
        // System.out.println("M: " + m + " B: " + b);
        // Compute predictions and gradient components

        // double error = y - prediction;

        double[] X = { 1.0, x, x2, x3, x4 };

        // Calculate gradient contribution
        double[] gradient = new double[5];
        double prediction;
        double sum = 0;
        for (int i = 0; i < X.length; i++) {
            sum += X[i] * parameters[i];
        }
        prediction = sum;

        // double prediction = m * X + b;
        double error = prediction - y;

        for (int i = 0; i < 5; i++) {
            gradient[i] = X[i] * error;
        }

        context.write(new Text("gradient"), new DoubleArrayWritable(gradient));

    }
}
