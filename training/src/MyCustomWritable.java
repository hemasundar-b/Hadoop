import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class MyCustomWritable implements WritableComparable<MyCustomWritable>{
	
	private String ename;
	private Long sal;
	private Integer dept;

	public MyCustomWritable(){
	}
	
	public MyCustomWritable(String ename, long sal, int dept ){
		
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
	
	@Override
	public int compareTo(MyCustomWritable o) {
		int cmp = this.dept.compareTo(o.dept);
		if (cmp == 0) {
			cmp = this.sal.compareTo(o.sal);	
		}
		
		return cmp;
	}
}
