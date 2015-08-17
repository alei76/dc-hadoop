package net.digitcube.hadoop.mapreduce.level;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class UpgradeTimeStatReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {

	private IntWritable mapValObj = new IntWritable();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int avgDuration = 0;
		int count = 0;
		if(Constants.SUFFIX_LEVEL_UP_TIME_STAT.equals(key.getSuffix())){ //升级平均时长统计
			for(IntWritable val : values){
				avgDuration += val.get();
				count++;
			}
			
			avgDuration = avgDuration/count; //单位 秒
			mapValObj.set(avgDuration);
			context.write(key, mapValObj);
			
		}else{ //升级时长区间分布统计
			for(IntWritable val : values){
				count++;
			}
			
			mapValObj.set(count);
			context.write(key, mapValObj);
		}
	}
}
