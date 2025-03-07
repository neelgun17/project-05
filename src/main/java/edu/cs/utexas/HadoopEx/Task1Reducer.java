package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  private double sumX, sumY, sumX2, sumXY, count;

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
    switch (text.toString()) {
      case "distance":
          sumX = sum;
          break;
      case "fare_amount":
          sumY = sum;
          break;
      case "distance_squared":
          sumX2 = sum;
          break;
      case "distance_fare_product":
          sumXY = sum;
          break;
      case "counts":
          count = sum;
          break;
  }

    // double b_num = (distance_squared * fare_amount_dbl) - (distance_dbl *
    // distance_fare_product);
    // b_num /= (count * distance_squared) - (distance_dbl * distance_dbl);

    context.write(text, new DoubleWritable(sum));
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
      double numerator = (count * sumXY) - (sumX * sumY);
      double denominator = (count * sumX2) - (sumX * sumX);

      if (denominator != 0) {
          double m = numerator / denominator;
          double b = (sumY - m * sumX) / count;
          context.write(new Text("m"), new DoubleWritable(m));
          context.write(new Text("b"), new DoubleWritable(b));
      } else {
          // Handle division by zero (e.g., all x-values are identical)
          // context.write(new Text("Error"), new Text("Undefined slope (denominator is zero)"));
      }
  }


}