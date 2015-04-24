package TopNRanking;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TopNRankingTrxnSum extends Configured implements Tool {

/*In the below credit card transaction toy dataset , rank the records with highest amount of total transaction per user in a month
 * Input:
 * cardnum|transAmt|transDate(YYYYMMDD)|
 * 100|10|20150101
 * 200|10|20150101
 * 100|20|20150101
 * 200|20|20150102
 * 100|10|20150102
 * 300|50|20150201
 * 100|30|20150202
 * 
 * Output:
 * 100|40|month-1|1
 * 200|30|month-1|2
 * 300|50|month-2|1
 * 100|30|month-2|2
 */
	
	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
	
		Job job = new Job(getConf(),getClass().getSimpleName());
		job.setJarByClass(getClass());
		job.setJobName("TopNRankingTrxnSum");
		
		job.setMapperClass(TopNMapperTrxnSum.class);
		job.setReducerClass(TopNReducerTrxnSum.class);
		job.setSortComparatorClass(SortComparator.class);
		
		job.setMapOutputKeyClass(CustomKeyValuePair.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(CustomKeyValuePair.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Configuration(), new TopNRankingTrxnSum(), args);
		System.exit(exitCode);
	}

}
