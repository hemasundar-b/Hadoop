import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadReviewsDriver extends Configured implements Tool {

@SuppressWarnings("deprecation")
@Override
public int run(String[] args) throws Exception {
  	
  	  Job job = new Job(getConf(),getClass().getSimpleName());
      job.setJarByClass(getClass());
      job.setJobName("Read Reviews Driver");

      job.setMapperClass(ReadReviewMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);

      job.setNumReduceTasks(0);
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      DistributedCache.addCacheFile(new Path(args[2]).toUri(),job.getConfiguration()); 
      
      return job.waitForCompletion(true) ? 0 : -1;

}

public static void main(String[] args)  throws Exception {
    /*if (args.length != 2) {
        System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
        System.exit(-1);
      }*/
	int exitCode = ToolRunner.run(new ReadReviewsDriver(), args);
	System.exit(exitCode);
}
}

