package TopNRanking;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {

	protected GroupComparator() {
		super(CustomTotTrxnPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CustomTotTrxnPair obj1 = (CustomTotTrxnPair) a;
		CustomTotTrxnPair obj2 = (CustomTotTrxnPair) b;
		int cmp = obj1.getMonth().compareTo(obj2.getMonth());
		return cmp;
	}
	
	

}
