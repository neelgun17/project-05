package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer extends  Reducer<Text, Text, Text, Text> {
   
   public void reduce(Text text, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {
	   
        float a=0;
        float b=0;
        float c=0;
        float d=0;
       
       for (Text value : values) {
           String[] split = value.toString().split(",");
            a += Float.parseFloat(split[0]);
            b += Float.parseFloat(split[1]);
            c += Float.parseFloat(split[2]);
            d += Float.parseFloat(split[3]);

       }
       
       context.write(text, new Text(a + "," + b + "," + c + "," + d));
   }
}