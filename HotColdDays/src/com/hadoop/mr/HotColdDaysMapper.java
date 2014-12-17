package com.hadoop.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HotColdDaysMapper extends Mapper<LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		String line = value.toString();
		String[] tokens = line.split(" ");
		
		String date = tokens[1];
		System.out.println("HotColdDaysMapper.map(): date: " + date + ", maxTemp: " + tokens[5] + ", minTemp: " + tokens[6]);
		float maxTemp = Float.parseFloat(tokens[5]);
		float minTemp = Float.parseFloat(tokens[6]);
		
		String outputValue = "";
		
		if(maxTemp > 40F) {
			outputValue = "A Hot Day ";
		}
		
		if(minTemp < 10) {
			outputValue += "A Hot Day";
		}
		
		context.write(new Text(date), new Text(outputValue));
	}
	
	public void run(org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {};
}
