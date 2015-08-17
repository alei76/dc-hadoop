package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑
 * 统计每关卡的关卡：独立玩家数、开始次数，成功次数，失败次数，失败退出次数，关卡平均时长
 * 失败退出率 = 失败时退出次数/失败次数
 * 失败率 = 失败次数/关卡开始次数
 * 
 * 输入：
 * appID, platform, channel, gameServer, accountId, levelId,  
 * dataFlag[BT(beginTimes),ET(endTimes)...], value[beginTimes, successTime, failedTimes, failedExitTimes, totalDuration]
 * 
 * 输出：
 * appID, platform, channel, gameServer, levelId
 * totalPlayerNum, beginTimes, successTimes, failedTimes, failedExitTimes, avgDuration
 */

public class GuanKaStatMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	String fileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appID = array[i++];
		String platform = array[i++];
		String channel = array[i++];
		String gameServer = array[i++];
		String accountId = array[i++];
		
		String levelId = array[i++];
		String seqno = array[i++];
		String dataFlag = array[i++];
		String times = array[i++];
		String[] keyFields = new String[]{
				appID,
				platform,
				channel,
				gameServer,
				levelId,
				seqno
		};
		String[] valFields = new String[]{
				accountId,
				dataFlag,
				times
		};
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(valFields);
		context.write(mapKeyObj, mapValObj);
		
		/*
		 * 20140828 ：上游 MR 已经把全服统计输出，因为还有其它 MR 依赖上游 MR
		 * 因此这里不用再输出全服数据
		 * 
		 * //全服
		keyFields[3] = MRConstants.ALL_GAMESERVER;
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(valFields);
		context.write(mapKeyObj, mapValObj);*/
		
	}
}
