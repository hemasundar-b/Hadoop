package TopNRanking;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomTotTrxnPair implements WritableComparable<CustomTotTrxnPair>{

	private Integer cardnum;
	private Integer month;
	private Long totalTrxnAmt;
	
	public CustomTotTrxnPair(){
		
	}
	
	public CustomTotTrxnPair(Integer cardnum, Integer month, Long totalTrxnAmt){
		this.cardnum = cardnum;
		this.month = month;
		this.totalTrxnAmt = totalTrxnAmt;
	}
	
	public Integer getMonth(){
		return this.month;
	}
	
	public Integer getCardnum(){
		return this.cardnum;
	}
	
	public Long totalTrxnAmt(){
		return this.totalTrxnAmt;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		cardnum = in.readInt();
		month = in.readInt();
		totalTrxnAmt = in.readLong();	
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(cardnum);
		out.writeInt(month);
		out.writeLong(totalTrxnAmt);
	}

	@Override
	public int compareTo(CustomTotTrxnPair o) {
		int cmp = this.month.compareTo(o.month);
		if (cmp == 0) {
			cmp = -1 * this.totalTrxnAmt.compareTo(o.totalTrxnAmt);
		}
		return cmp;
	}
	
	public String toString(){
		 return this.month.toString() + "|" + this.cardnum.toString() + "|" + this.totalTrxnAmt.toString() ;
	}

}
