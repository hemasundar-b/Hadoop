package Joins;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


public class Dept implements Writable{
	
	private Integer dept;
	private String dname;
	
	public Dept(){
	}
	
	public Dept(Integer dept,String dname ){
		
		this.dept = dept;
		this.dname = dname;
	}

	public Integer getDept(){
		return this.dept;
	}
	
	public String getDname(){
		return this.dname;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		dept = in.readInt();
		dname = WritableUtils.readString(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(dept);
		WritableUtils.writeString(out,dname);
	}
	
	public String toString(){
		return dept.toString() +  "|" + dname.toString() ;
		
	}
}
