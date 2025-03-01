package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer extends  Reducer<Text, Text, Text, Text> {

   public void reduce(Text text, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {
	   
        float distance_float = 0;
        float fare_amount_float = 0;
        float distance_fare_product = 0;
        float distance_squared = 0;

        float count = 0;
         
       for (Text value : values) {
            // System.err.println(value);
           String[] split = value.toString().split(",");
           distance_float += Float.parseFloat(split[0]);
           fare_amount_float += Float.parseFloat(split[1]);
           distance_fare_product += Float.parseFloat(split[2]);
           distance_squared += Float.parseFloat(split[3]);
           count += Float.parseFloat(split[4]);
       }

      float m_num = (count * distance_fare_product) - (distance_float * fare_amount_float);
      m_num /= (count * distance_squared) - (distance_float * distance_float);

      float b_num = (distance_squared * fare_amount_float) - (distance_float * distance_fare_product);
      b_num /= (count * distance_squared) - (distance_float * distance_float);
       
       context.write(text, new Text(m_num + "," + b_num));
   }


}