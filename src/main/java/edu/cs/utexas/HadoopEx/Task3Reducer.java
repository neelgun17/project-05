package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;

public class Task3Reducer extends Reducer<Text, DoubleArrayWritable, Text, DoubleArrayWritable> {

    public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        double learningRate = 0.001;
        int featureCount = 5;
        double[] gradientSum = new double[featureCount];
        int sampleCount = 0;
        // double[] parameters = { 0.1, 0.1, 0.1, 0.1, 0.1 };

        // Aggregate gradients
        for (DoubleArrayWritable partialGradient : values) {
            DoubleWritable[] gradient = partialGradient.get();
            for (int i = 0; i < featureCount; i++) {
                gradientSum[i] += gradient[i].get();
            }
            sampleCount++;
        }

        // Update parameters
        // if (sampleCount > 0) {
        //     for (int i = 0; i < featureCount; i++) {
        //         parameters[i] -= learningRate * (gradientSum[i] / sampleCount);
        //     }
        // }

        // if (key.toString().equals("dm")) {
        //     context.getCounter("Gradient", "dm").increment((long) (sum * 1e6)); // Scale up by 1e6 to avoid precision loss
            
        // } else if (key.toString().equals("db")) {
        //     context.getCounter("Gradient", "db").increment((long) (sum * 1e6)); // Scale up by 1e6 to avoid precision loss
        // }
        context.getCounter("Gradient", "m").increment((long) (gradientSum[1] * 1e6));
        context.getCounter("Gradient", "m2").increment((long) (gradientSum[2] * 1e6));
        context.getCounter("Gradient", "m3").increment((long) (gradientSum[3] * 1e6));
        context.getCounter("Gradient", "m4").increment((long) (gradientSum[4] * 1e6));
        context.getCounter("Gradient", "b").increment((long) (gradientSum[0] * 1e6));
        context.getCounter("Gradient", "c").increment((long) sampleCount);
        // context.write(key, new DoubleWritable(sum));

        // Emit new parameters
        context.write(new Text("params"), new DoubleArrayWritable(gradientSum));
    }
}
