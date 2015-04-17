import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class MyCustomWritable implements Writable{
	
	private Text ename;
	private LongWritable sal;
	private IntWritable dept;

	
	public MyCustomWritable(){
		ename = new Text();
		sal = new LongWritable();
		dept = new IntWritable();
	}
	
	public MyCustomWritable(Text ename, LongWritable sal, IntWritable dept ){
		
		this.ename = ename;
		this.sal = sal;
		this.dept = dept;
	}

	
	public LongWritable getsal(){
		return this.sal;
	}

	public IntWritable getDept(){
		return this.dept;
	}
	
	public Text getEname(){
		return this.ename;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		ename.readFields(in);
		sal.readFields(in);
		dept.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		ename.write(out);
		sal.write(out);
		dept.write(out);
	}
	
	public String toString(){
		return ename.toString() + "|" +  sal.toString() + "|"+  dept.toString()   ;
		
	}

}
