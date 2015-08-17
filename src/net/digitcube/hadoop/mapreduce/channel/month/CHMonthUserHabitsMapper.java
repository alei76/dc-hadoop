package net.digitcube.hadoop.mapreduce.channel.month;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.DateUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CHMonthUserHabitsMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	
	private int statDate = 0;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statDate = DateUtil.getStatMonthDate(context.getConfiguration()); 
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = arr[i++];
		String appVer = arr[i++];
		String platform = arr[i++];
		String channel = arr[i++];
		String country = arr[i++];
		String province = arr[i++];
		String uid = arr[i++];
		boolean isFirstLogin = "1".equals(arr[i++]);
		boolean isFirstPay = "1".equals(arr[i++]);
		String loginTimes = arr[i++];
		String onlineTime = arr[i++];
		String onlineDay = arr[i++];
		String currencyAmount = arr[i++];
		String payTimes = arr[i++];
		
		// 新增/活跃玩家游戏时长及次数
		String[] keyFields = new String[]{
				statDate+"",
				appId,
				appVer,
				platform,
				channel,
				country,
				province,
				Constants.PLAYER_TYPE_ONLINE
		};
		String[] valFields = new String[]{
				loginTimes,
				onlineTime
		};
		
		mapKeyObj.setOutFields(keyFields);
		mapValueObj.setOutFields(valFields);
		context.write(mapKeyObj, mapValueObj);
		
		// 新增玩家游戏时长及次数
		if(isFirstLogin){
			keyFields[keyFields.length-1] = Constants.PLAYER_TYPE_NEWADD;
			context.write(mapKeyObj, mapValueObj);
		}
	}
}
