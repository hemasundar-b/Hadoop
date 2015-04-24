package TopNRanking;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TopNMapperTrxnSum extends Mapper<LongWritable, Text, CustomKeyValuePair, LongWritable> {

	private CustomKeyValuePair keyObj ;
	private Integer cardnum;
	private Integer month;
	private Long transactionAmt;
	private LongWritable trxnAmt = new LongWritable();
	
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
	
		String[] entireRec = value.toString().split("\\|");
		cardnum = Integer.parseInt(entireRec[0]);
		month = Integer.parseInt(entireRec[2].substring(4,6));
		transactionAmt = Long.parseLong(entireRec[1]);
		keyObj = new CustomKeyValuePair(cardnum, month,transactionAmt);
		trxnAmt.set(transactionAmt);
		context.write(keyObj,trxnAmt);
		
	}

}
