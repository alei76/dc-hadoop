package net.digitcube.hadoop.mapreduce.onlinenew;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author rickpan
 * @version 1.0 
 * <br>
 * 输入：
 * a) 在线玩家当前小时去重结果
 * 	  APPID,Platform,GameServer,AccountID,'CNT'(字符串标志), 1
 * 
 * b) ACU 每个时间点在线人数结果
 * 	  APPID,Platform,GameServer,TimePoint,'ACU'(字符串标志), sum(1)(该时间点在线人数)
 * 
 * c) PCU 每秒钟在线人数结果
 * 	  APPID,Platform,GameServer,TimePoint,'PCU'(字符串标志), sum(1)(该时间点在线人数)
 * 
 * 输出：
 * key : APPID,Platform,GameServer
 * value : onlineCount(在线人数),['CNT','ACU','PCU']
 * 
 */

public class AcuPcuCntSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] valueArr = value.toString().split(MRConstants.SEPERATOR_IN);
		
		String appId = valueArr[0];
		String platform = valueArr[1];
		String gameServer = valueArr[2];
		
		String dataFlag = valueArr[4];
		String playCount = valueArr[5];
		
		String[] keyFields = new String[] {appId, platform, gameServer};
		String[] valFields = new String[] {playCount, dataFlag};
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(valFields);
		
		context.write(mapKeyObj, mapValObj);
	}
}
