package Joins;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapSideJoinDistCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private Integer dept;
	private String dname;
	private String entireOutRecVar;
	private Text entireOutRec = new Text();
	private Map<Integer, String> deptMap = new HashMap<Integer, String>();
	private static final Log log = LogFactory.getLog(MapSideJoinDistCacheMapper.class);
	
	@Override
	public void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String inputRec = value.toString(); 
		Integer deptid = Integer.parseInt(inputRec.split("\\|")[2]);
		log.info("deptid from employee file " + deptid );
		if (deptMap.containsKey(deptid)){
			String dnameVar = deptMap.get(deptid);
			entireOutRecVar = inputRec + "|" + dnameVar; 
		}else{
			entireOutRecVar = inputRec + "|" + "";
		}
		entireOutRec.set(entireOutRecVar);
		context.write(entireOutRec, NullWritable.get());
	}

	@SuppressWarnings("deprecation")
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {

			Path[] cacheFiles = context.getLocalCacheFiles();
			for (Path cacheFile : cacheFiles){
				String deptFile = cacheFile.toString();
				readCacheFiles(deptFile);
			}
	}

	
	public void readCacheFiles(String deptFile) throws IOException{
		
		String line = null;
		try {
			BufferedReader br = new BufferedReader(new FileReader(deptFile));
			while ((line = br.readLine()) != null){
				String[] entireRecArr = line.split("\\|");
				dept = Integer.parseInt(entireRecArr[0]);
				dname = entireRecArr[1];
				log.info(dept + "|" + dname);
				deptMap.put(dept, dname);
			}
			
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
