import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AggregateReducer extends Reducer<IntWritable, MyCustomWritable, NullWritable, Text>{

	
	@Override
	public void reduce(IntWritable key, Iterable<MyCustomWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Text maxRec = new Text();
		Long maxSal = null ;
		Long salVar ;
		
		for ( MyCustomWritable obj : values){
			salVar = obj.getsal().get();
			if(maxSal == null ||  salVar > maxSal){
				maxSal = salVar;
				maxRec.set(obj.toString());
			}
		}
		context.write(null,maxRec);
	}

	
	
	
}