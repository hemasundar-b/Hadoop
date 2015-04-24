package Joins;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapSideJoinDistCacheMapper extends
		Mapper<LongWritable, Text, Employee, NullWritable> {

}
