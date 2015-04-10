import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import java.util.HashSet;
import java.util.Set;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.StringTokenizer;

public class ReadReviewMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	private Set<String> stopWordsSet = new HashSet<String>();
	private BufferedReader br ;
	private static final IntWritable one = new IntWritable(1); 
	
	protected void readStopWords(Path stopWordFile) throws IOException, InterruptedException{
		
		try {
			String line = null;
			br = new BufferedReader(new FileReader(stopWordFile.toString()));
			while ( (line = br.readLine()) != null){
				System.out.println(line);
				stopWordsSet.add(line);
			}
		} catch (Exception e){
			System.err.println("Not able to read the cache files" + e);
		}
		
	}
	
	@SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		try {
			Path[] stopWordPath = context.getLocalCacheFiles();
			for (Path stopWordPathVar : stopWordPath){
				readStopWords(stopWordPathVar);
			}
			
		} catch (Exception e){
			System.err.println("Not able to get the cache files" + e);
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		StringTokenizer str = new StringTokenizer(value.toString());
		
		while (str.hasMoreTokens()){
			String token = str.nextToken();
			if (stopWordsSet.contains(token)){
			}	
			else {
				context.write(new Text(token), one);
			} 
		}
	}
}
