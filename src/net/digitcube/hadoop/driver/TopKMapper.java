package net.digitcube.hadoop.driver;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.TreeMap;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 该 MR 实现两个功能
 * 1, 注册与激活玩家分离
 * 2, 对同一台设备的激活去重（同一台设备可能会上报多次）
 * 以 appid, platform, uid 为 key 
 *
 */
public class TopKMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	TreeMap<Integer,Text> st = new TreeMap<Integer,Text>();
	Text key = new Text("Key");
	IntWritable mapVal = new IntWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		int val = Integer.parseInt(value.toString());
		if(st.size()<10){
			st.put(val, value);
		}else{
			Integer first = st.firstKey();
			if(first < val){
				st.pollFirstEntry();
				st.put(val, value);
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Set<Integer> iSet = st.keySet();
		for(Integer i : iSet){
			mapVal.set(i);
			context.write(key, mapVal);
		}
		super.cleanup(context);
	}
	
}
