package Joins;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

public class MapSideJoinDistCacheMapper extends Mapper<LongWritable, Text, Employee, NullWritable> {

	private Integer dept;
	private String dname;
	private List<Dept> al = new ArrayList<Dept>();
	
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
	}

	@SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		String line = null;
		Path[] cacheFiles = context.getLocalCacheFiles();
		String deptFile = cacheFiles[0].toString();
		BufferedReader br = new BufferedReader(new FileReader(deptFile));
		while ((line = br.readLine()) != null){
			String[] entireRecArr = line.split("\\|");
			dept = Integer.parseInt(entireRecArr[0]);
			dname = entireRecArr[1];
			Dept deptObj = new Dept(dept,dname);
			al.add(deptObj);
			
		}
		br.close();
	}

}
