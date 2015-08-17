package net.digitcube.hadoop.driver;

import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKReducer extends Reducer<Text, IntWritable, IntWritable, NullWritable> {

	TreeMap<Integer,Text> st = new TreeMap<Integer,Text>();
	IntWritable redKey = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		for(IntWritable value : values){
			
			if(st.size()<10){
				st.put(value.get(), key);
			}else{
				Integer first = st.firstKey();
				if(first < value.get()){
					st.pollFirstEntry();
					st.put(value.get(), key);
				}
			}
		}
	}

	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
		
		Set<Integer> iSet = st.keySet();
		for(Integer i : iSet){
			redKey.set(i);
			context.write(redKey, NullWritable.get());
		}
		
		super.cleanup(context);
	}
	
	
}
