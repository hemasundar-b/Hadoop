package TopNRanking;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {

	protected SortComparator() {
		super(CustomKeyValuePair.class,true);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int compare( WritableComparable a, WritableComparable b) {
		CustomKeyValuePair obj1 = (CustomKeyValuePair) a;
		CustomKeyValuePair obj2 = (CustomKeyValuePair) b;
		
		int cmp = obj1.getMonth().compareTo(obj2.getMonth());
		if (cmp == 0){
			cmp = obj1.getcardnum().compareTo(obj2.getcardnum());
		}
			
		return cmp;
	}	
}
