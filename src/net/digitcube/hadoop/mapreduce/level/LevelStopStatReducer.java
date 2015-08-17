package net.digitcube.hadoop.mapreduce.level;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class LevelStopStatReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, NullWritable> {

	private Set<Integer> levelSet = new HashSet<Integer>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		levelSet.clear();
		int loginTimes = 0;
		for(IntWritable level : values){
			loginTimes++;
			levelSet.add(level.get());
		}
		
		// totalNum == 3，有三条记录，说明三天  OnlineDay 都有该玩家记录，该玩家三天都登录了
		// 1 == levelSet.size() 说明三天里该玩家级别没有变化
		if(loginTimes == 3 && 1 == levelSet.size()){ 
			for(Integer level : levelSet){ // 只会有一个 level
				key.getOutFields()[key.getOutFields().length - 1] = level.toString();
			}
			
			key.setSuffix(Constants.SUFFIX_LEVEL_STOP_STAT);
			context.write(key, NullWritable.get());
		}
	}
}
