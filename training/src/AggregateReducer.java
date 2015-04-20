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
		Text minRec = new Text();
		Long minSal = null ;
		Long maxSal = null ;
		Long salVar ;
		
		for ( MyCustomWritable obj : values){
			salVar = obj.getsal();
			if(maxSal == null ||  salVar > maxSal){
				maxSal = salVar;
				maxRec.set(obj.toString());
			}
			
			if (minSal == null || salVar < minSal){
				minSal = salVar;
				minRec.set(obj.toString());
			}
			
		}
		context.write(null,maxRec);
		context.write(null,minRec);
	}

	
	
	
}