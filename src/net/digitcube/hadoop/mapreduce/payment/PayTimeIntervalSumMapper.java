package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 输入：
 * 每个玩家首充、二充、三充时间间隔(@UserInfoRollingDayReducer 后缀 PAY_TIME_INTERVAL )
 * 
 * Map:
 * key : appid, platform, channel, gameServer, type[pti1,pti2,pti3], vkey[1~10m/2~4h]
 * val : 1
 * 
 * Reduce:
 * appid, platform, channel, gameServer, type[pti1,pti2,pti3], vkey[1~10m/2~4h], sum(1)
 * 
 */

public class PayTimeIntervalSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private static final IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] valueArr = value.toString().split(MRConstants.SEPERATOR_IN);
		
		String appId = valueArr[0];
		String platform = valueArr[1];
		String channel = valueArr[2];
		String gameServer = valueArr[3];
		
		//首充、二充、三充标识 pti1,pti2,pti3
		String payTimeIntervType = valueArr[5]; 
		//首充、二充、三充时间间隔范围
		String payTimeIntervRange = valueArr[6]; 
		
		String[] keyFields = new String[] {appId, platform, channel, gameServer, payTimeIntervType, payTimeIntervRange};
		mapKeyObj.setOutFields(keyFields);
		
		context.write(mapKeyObj, one);
	}
}

