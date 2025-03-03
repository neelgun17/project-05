package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1Mapper extends Mapper<Object, Text, Text, DoubleWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] split = value.toString().split(",");
		if (split.length == 17) {

			String trip_time = split[4];
			String fare_amount = split[11];
			String trip_distance = split[5];
			String toll_amount = split[15];
			for (int i = 0; i < 17; i++) {
				if ("".equals(split[i])) {
					return;
				}
			}

			try {
				double time = Double.parseDouble(trip_time);
				time /= 60;
				if (!(time > 2 && time < 60)) {
					return;				
				}
			} catch (Exception e) {
				// TODO: handle exception
			} 

			try {
				double fare = Double.parseDouble(fare_amount);
				if (!(fare > 3 && fare < 200)) {
					return;
				}
			} catch (Exception e) {
				// TODO: handle exception

			}
			try {
				double dist = Double.parseDouble(trip_distance);
				if (!(dist > 1 && dist < 50)) {
					return;
				}
			} catch (Exception e) {
				// TODO: handle exception
				
			}
			try {
				double toll = Double.parseDouble(toll_amount);
				if (!(toll > 3)) {
					return;
				}
			} catch (Exception e) {
				// TODO: handle exception
				
			}

			double distance_Double = Double.parseDouble(trip_distance);
			double fare_amount_Double = Double.parseDouble(fare_amount);
			double distance_fare_product = distance_Double * fare_amount_Double;
			double distance_squared = distance_Double * distance_Double;

			// System.err.println(distance_Double + "," + fare_amount_Double + "," + distance_fare_product + "," + distance_squared);

			// context.write(new Text("a"), new Text(distance_Double + "," + fare_amount_Double + "," + distance_fare_product + "," + distance_squared + ",1"));
			context.write(new Text("distance"), new DoubleWritable(distance_Double));
			context.write(new Text("fare_amount"), new DoubleWritable(fare_amount_Double));
			context.write(new Text("distance_fare_product"), new DoubleWritable(distance_fare_product));
			context.write(new Text("distance_squared"), new DoubleWritable(distance_squared));

			
		}
	}
}