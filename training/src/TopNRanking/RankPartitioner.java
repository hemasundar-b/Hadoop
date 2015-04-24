package TopNRanking;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RankPartitioner extends Partitioner<CustomTotTrxnPair, NullWritable> {

	@Override
	public int getPartition(CustomTotTrxnPair key, NullWritable value, int numReduceTasks) {

		return key.getMonth().hashCode() % numReduceTasks;
	}
}
