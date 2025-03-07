package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  public void reduce(Text text, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {

    // double distance_dbl = 0;
    // double fare_amount_dbl = 0;
    // double distance_fare_product = 0;
    // double distance_squared = 0;

    // double count = 0;
    double sum = 0;

    for (DoubleWritable value : values) {

      // if (text.toString().equals("distance")) {
      // distance_dbl += value.get();
      // }
      // else if (text.toString().equals("fare_amount")) {
      // fare_amount_dbl += value.get();
      // }
      // else if (text.toString().equals("distance_fare_product")) {
      // distance_fare_product += value.get();
      // }
      // else {
      // distance_squared += value.get();
      // }
      sum += value.get();
    }

    // double m_num = (count * distance_fare_product) - (distance_dbl *
    // fare_amount_dbl);
    // m_num /= (count * distance_squared) - (distance_dbl * distance_dbl);

    // double b_num = (distance_squared * fare_amount_dbl) - (distance_dbl *
    // distance_fare_product);
    // b_num /= (count * distance_squared) - (distance_dbl * distance_dbl);

    context.write(text, new DoubleWritable(sum));
  }

}