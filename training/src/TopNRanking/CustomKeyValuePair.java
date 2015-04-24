package TopNRanking;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomKeyValuePair implements WritableComparable<CustomKeyValuePair> {

	private Integer cardnum;
	private Integer month ;
	private Long transactionAmt;
	
	public CustomKeyValuePair(){
	}
	
	public CustomKeyValuePair(Integer cardnum, Integer month, Long transactionAmt){
		this.cardnum = cardnum;
		this.month = month;
		this.transactionAmt = transactionAmt;
	}

	public Integer getcardnum() {
		return cardnum;
	}

	public Integer getMonth() {
		return month;
	}

	public Long gettransactionAmt(){
		return transactionAmt;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		cardnum = in.readInt();
		month = in.readInt();
		transactionAmt = in.readLong();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(cardnum);
		out.writeInt(month);
		out.writeLong(transactionAmt);
	}

	@Override
	public int compareTo(CustomKeyValuePair o) {
		int cmp = this.getcardnum().compareTo(o.getcardnum());
		if (cmp == 0){
			cmp =  this.getMonth().compareTo(o.getMonth());
		}
		return cmp;
	}
	
	public String toString(){
		return getcardnum() + "|" + getMonth() ;
	}
}
