package net.digitcube.hadoop.mapreduce.level;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author rickpan
 * @version 1.0 
 * 
 * 主要逻辑：
 * 把前三天的玩家在线信息作为输入
 * 以 appid, platform, uid 为 key 进行关联，并输出 level
 * 在 reduce 端判断，如果该 key 关联到的 level 没有变化
 * 则说明是等级停滞玩家，将其输出
 * 
 * 输入：
 * 前三天的玩家在线日志
 * 
 * Map
 * Key:	appid, platform, channel, gameserver, accountId
 * Val: level
 * 
 * 输出:
 * 		appid, platform, channel, gameserver, level 
 * 
 * 
 */
public class LevelStopStatMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private IntWritable mapValObj = new IntWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		OnlineDayLog onlineDayLog = new OnlineDayLog(array);
		String appId = onlineDayLog.getAppID();
		String platform = onlineDayLog.getPlatform();
		String accountId = onlineDayLog.getAccountID();
		String channel = onlineDayLog.getExtend().getChannel();
		String gameServer = onlineDayLog.getExtend().getGameServer();
		String dayMaxLevel = onlineDayLog.getMaxLevel()+"";
		
		
		String[] keyFields = new String[] {appId, platform, channel, gameServer, accountId};
		
		mapKeyObj.setOutFields(keyFields);
		mapValObj.set(StringUtil.convertInt(dayMaxLevel,0));
		
		context.write(mapKeyObj, mapValObj);
	}
}
