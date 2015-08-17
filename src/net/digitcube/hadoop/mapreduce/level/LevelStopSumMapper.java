package net.digitcube.hadoop.mapreduce.level;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeSet;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑：
 * 
 * 统计各个等级停滞的人数
 * 
 */

public class LevelStopSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private String fileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		
		if(fileName.contains(Constants.SUFFIX_LEVEL_STOP_STAT)){
			//appid, platform, channel, gameserver, level
			//升级滞停分布统计
			String appid = array[0];
			String platform = array[1];
			String channel = array[2];
			String gameServer = array[3];
			//String level = array[4];
			//等级默认用 1 级
			int level = StringUtil.convertInt(array[4], 0);
			level = 0 == level ? 1 : level;
			
			String[] accNumPerDev = new String[]{appid, platform, channel, gameServer, 
												 Constants.PLAYER_TYPE_ONLINE, 
												 Constants.DIMENSION_LEVEL_STOP, 
												 ""+level};
			
			mapKeyObj.setOutFields(accNumPerDev);
			context.write(mapKeyObj, one);
		}
	}
}
