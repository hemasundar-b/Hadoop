package TopNRanking;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopNRankJob extends Configured implements Tool {
	

	public static class RankMapper extends Mapper<LongWritable,Text,CustomTotTrxnPair,NullWritable> {

		private String[] entireInRec;
		private Integer cardnum;
		private Integer month;
		private Long totTrxnAmt;
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			entireInRec = value.toString().split("\\|");
			cardnum = Integer.parseInt(entireInRec[0]);
			month = Integer.parseInt(entireInRec[1]);
			totTrxnAmt = Long.parseLong(entireInRec[2]);
			CustomTotTrxnPair keyPair = new CustomTotTrxnPair(cardnum, month, totTrxnAmt);
			context.write(keyPair, NullWritable.get());
		}
	}
	
	public static class RankReducer extends Reducer<CustomTotTrxnPair,NullWritable ,CustomTotTrxnPair, IntWritable>{
		
		private Integer rank;
		
		@SuppressWarnings("unused")
		@Override
		protected void reduce(CustomTotTrxnPair key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException {
			
			rank = 1;
			for(NullWritable val : values){
				context.write(key, new IntWritable(rank));
				rank++;
			}	
		}	
	}
	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), getClass().getSimpleName());
		job.setJarByClass(getClass());
		job.setJobName("TopNRanking");
		job.setMapperClass(RankMapper.class);
		job.setReducerClass(RankReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(CustomTotTrxnPair.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(CustomTotTrxnPair.class);
		job.setOutputValueClass(IntWritable.class);
		job.setPartitionerClass(RankPartitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Configuration(), new TopNRankJob(), args);
		System.exit(exitCode);
	}

}
