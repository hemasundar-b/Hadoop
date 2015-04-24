package TopNRanking;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducerTrxnSum extends Reducer<CustomKeyValuePair, LongWritable, Text, NullWritable> {
	
	private Long totalTrxnAmt ;
	private Text outputRec = new Text();
	private String outRec ;
	
	@Override
	protected void reduce(CustomKeyValuePair Key, Iterable<LongWritable> trxnValues ,Context context)
			throws IOException, InterruptedException {
		
		totalTrxnAmt = 0L;
		for (LongWritable trxnAmt : trxnValues) {
			totalTrxnAmt = totalTrxnAmt + trxnAmt.get();
		}		
		outputRec.set(outRec);
		context.write(outputRec,NullWritable.get());
	}
}
