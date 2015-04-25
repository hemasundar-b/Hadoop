package Joins;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


public class Employee implements Writable{
	
	private String ename;
	private Long sal;
	private Integer dept;

	public Employee(){
	}
	
	public Employee(String ename, long sal, int dept ){
		
		this.ename = ename;
		this.sal = sal;
		this.dept = dept;
	}
	
	public Long getsal(){
		return this.sal;
	}

	public Integer getDept(){
		return this.dept;
	}
	
	public String getEname(){
		return this.ename;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		ename = WritableUtils.readString(in);
		sal = in.readLong();
		dept = in.readInt();
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out,ename);
		out.writeLong(sal);
		out.writeInt(dept);
	}
	
	public String toString(){
		return ename.toString() + "|" +  sal.toString() + "|"+  dept.toString()   ;
		
	}
}
