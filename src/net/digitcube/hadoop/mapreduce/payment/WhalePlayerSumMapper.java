package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 统计每个玩家分别在版本、渠道、区服几个维度的付费总额
 *  
 * 输入：[appId,platform,dimenType,dimenKey,accountId,
 * 		  firstLoginDay,firstPayDay,payAmount,payTimes,onlineDays,onlineTimes,gameTimes,level]
 * 
 * Top 100 的鲸鱼玩家
 */
public class WhalePlayerSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = arr[i++];
		String platform = arr[i++];
		String dimenType = arr[i++];
		String dimenVkey = arr[i++];
		String accountId = arr[i++];
		
		String[] keyFields = new String[]{appId, platform, dimenType, dimenVkey};
		mapKeyObj.setOutFields(keyFields);
		mapValueObj.setOutFields(arr);
		context.write(mapKeyObj, mapValueObj);
	}
}
