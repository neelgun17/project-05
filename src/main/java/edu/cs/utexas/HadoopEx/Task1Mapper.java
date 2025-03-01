package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1Mapper extends Mapper<Object, Text, Text, Text> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] split = value.toString().split(",");
		String[] pickup = split[2].split(" ");
		String[] dropoff = split[3].split(" ");
		if (pickup.length == 2 && split.length == 17) {
			String[] pickup_time = pickup[1].split(":");
			String[] dropoff_time = dropoff[1].split(":");

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
				Float time = Float.parseFloat(trip_time);
				time /= 60;
				if (!(time > 2 && time < 60)) {
					return;				
				}
			} catch (Exception e) {
				// TODO: handle exception
			} 

			try {
				Float fare = Float.parseFloat(fare_amount);
				if (!(fare > 3 && fare < 200)) {
					return;
				}
			} catch (Exception e) {
				// TODO: handle exception

			}
			try {
				Float dist = Float.parseFloat(trip_distance);
				if (!(dist > 1 && dist < 50)) {
					return;
				}
			} catch (Exception e) {
				// TODO: handle exception
				
			}
			try {
				Float toll = Float.parseFloat(toll_amount);
				if (!(toll > 3)) {
					return;
				}
			} catch (Exception e) {
				// TODO: handle exception
				
			}

			float distance_float = Float.parseFloat(trip_distance);
			float fare_amount_float = Float.parseFloat(fare_amount);
			float distance_fare_product = distance_float * fare_amount_float;
			float distance_squared = distance_float * distance_float;

			// System.err.println(distance_float + "," + fare_amount_float + "," + distance_fare_product + "," + distance_squared);

			context.write(new Text("a"), new Text(distance_float + "," + fare_amount_float + "," + distance_fare_product + "," + distance_squared + ",1"));


			
		}
	}
}