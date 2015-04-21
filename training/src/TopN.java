import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


/* @Author : Hemasundar Battina
 * Secondary Sort for finding topN salaried employees per department
 */

public class TopN extends Configured implements Tool{

	static class TopNMapper extends Mapper<LongWritable , Text, MyCustomWritable, NullWritable> {
		
		private String ename ;
		private Integer dept ;
		private Long sal ;
		private MyCustomWritable obj;
		
		@Override
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String[] entireRecArray = value.toString().split("\\|");
			ename = entireRecArray[0];
			sal =   Long.parseLong(entireRecArray[1]);
			dept =  Integer.parseInt(entireRecArray[2]);
			obj = new MyCustomWritable(ename,sal,dept);
			context.write(obj,NullWritable.get());
		}
	}
	
	static class TopNReducer extends Reducer<MyCustomWritable, NullWritable,Text,NullWritable>{
		
		Text entireRec = new Text();
		@Override
		protected void reduce(MyCustomWritable key ,Iterable<NullWritable> value,Context context)
				throws IOException, InterruptedException {
			entireRec.set(key.toString());
			context.write(entireRec,null);
			
		}
	}
	
	static class CustomPartitioner extends Partitioner<MyCustomWritable, NullWritable> {
		@Override
		public int getPartition(MyCustomWritable key, NullWritable value,int numPartitions) {
		return Math.abs(key.getDept() * 127) % numPartitions;
		}
	}
	
	static class GroupComparator extends WritableComparator{
		protected GroupComparator() {
			super(MyCustomWritable.class, true);
		}
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable i, WritableComparable j) {
			MyCustomWritable obj1 = (MyCustomWritable) i;
			MyCustomWritable obj2 = (MyCustomWritable) j;
			return obj1.getDept().compareTo(obj2.getDept());	
			}
		}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(),getClass().getSimpleName());
		job.setJarByClass(getClass());
		job.setJobName("TopN");
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		job.setMapOutputKeyClass(MyCustomWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new TopN(), args);
		System.exit(exitCode);
	}
}