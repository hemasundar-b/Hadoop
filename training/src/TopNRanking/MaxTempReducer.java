package TopNRanking;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

	LongWritable outValue = new LongWritable();
	
	@Override
	protected void reduce(IntWritable key, Iterable<LongWritable> value,Context context)
			throws IOException, InterruptedException {
		
		Long maxVar = null;	
		for (LongWritable values : value){
			if ( maxVar == null || values.get() > maxVar ){
				maxVar = values.get();
			}
		outValue.set(maxVar);
		}
		context.write(key, outValue);
	}
	
}
