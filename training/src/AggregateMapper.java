import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class AggregateMapper extends Mapper<LongWritable,Text,IntWritable,MyCustomWritable>{

	String ename ;
	Integer dept ;
	IntWritable deptKey = new IntWritable();
	Long sal ;
	MyCustomWritable obj;
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String[] entireRec = value.toString().split("\\|");
		ename = entireRec[0];
		sal =   Long.parseLong(entireRec[1]);
		dept =  Integer.parseInt(entireRec[2]);
		obj =   new MyCustomWritable(ename,sal,dept);
		deptKey.set(dept);
		context.write(deptKey,obj);
		
	}
	
	
}