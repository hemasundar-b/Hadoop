package TopNRanking;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
	
	IntWritable temperatureKey = new IntWritable();
	LongWritable temperatureValue = new LongWritable();
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		Integer tempValue ;
		String line = value.toString();
		
		Integer tempKey =Integer.parseInt(line.substring(15, 19));
		if (line.charAt(40) == '+'){
			tempValue =Integer.parseInt(line.substring(41, 45));	
		} else {
			tempValue =Integer.parseInt(line.substring(40, 45));
		}
		
		temperatureKey.set(tempKey);
		temperatureValue.set(tempValue);
		
		context.write(temperatureKey, temperatureValue);
		
	}
	
	

}
