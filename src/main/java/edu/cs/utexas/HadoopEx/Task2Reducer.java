package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;


public class Task2Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        for (DoubleWritable value : values) {
            sum += value.get();
            context.getCounter("Gradient", "c").increment((long)1);
        }

        // Use counters to store aggregated gradients
        if (key.toString().equals("dm")) {
            context.getCounter("Gradient", "dm").increment((long) (sum * 1e6)); // Scale up by 1e6 to avoid precision loss
            
        } else if (key.toString().equals("db")) {
            context.getCounter("Gradient", "db").increment((long) (sum * 1e6)); // Scale up by 1e6 to avoid precision loss
        }
        context.write(key, new DoubleWritable(sum));
    }
}
