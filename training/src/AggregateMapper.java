import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class AggregateMapper extends Mapper<LongWritable,Text,IntWritable,MyCustomWritable>{

	Text ename = new Text();
	IntWritable dept = new IntWritable();
	LongWritable sal = new LongWritable();
	MyCustomWritable obj;
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String[] entireRec = value.toString().split("\\|");
		ename.set(entireRec[0]);
		sal.set(Long.parseLong(entireRec[1]));
		dept.set(Integer.parseInt(entireRec[2]));
		obj = new MyCustomWritable(ename,sal,dept);
		context.write(dept,obj);
		
	}
	
	
	
	
}