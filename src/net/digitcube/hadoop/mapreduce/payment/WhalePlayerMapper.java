package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 统计每个玩家分别在版本、渠道、区服几个维度的付费总额 
 */
public class WhalePlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appIdVersion = arr[i++];
		String[] appInfo = appIdVersion.split("\\|");
		String appId = appInfo[0];
		String version = appInfo[1];
		
		String platform = arr[i++];
		String channel = arr[i++];
		String gameServer = arr[i++];
		String accountId = arr[i++];
		
		String firstLoginDay = arr[i++];
		String firstPayDay = arr[i++];
		String payAmount = arr[i++];
		String payTimes = arr[i++];
		String onlineDays = arr[i++];
		String onlineTimes = arr[i++];
		String gameTimes = arr[i++];
		String level = arr[i++];
		
		//区服需另外统计每天登录次数、在线时长及付费金额
		//然后根据该鲸鱼玩家前后几天的这几项指标判断其活跃度
		String loginTimesToday = arr[i++];
		String onlineTimeToday = arr[i++];
		String payAmountToday = arr[i++];
		
		String[] valFields = new String[]{
				firstLoginDay,
				firstPayDay,
				payAmount,
				payTimes,
				onlineDays,
				onlineTimes,
				gameTimes,
				level,
				loginTimesToday,
				onlineTimeToday,
				payAmountToday
		};
		
		mapValueObj.setOutFields(valFields);
		
		
		//版本维度付费统计
		//真实版本
		String[] keyFields = new String[]{appId, platform, Constants.DIMENSION_PAY_ON_VERSION, version, accountId};
		mapKeyObj.setOutFields(keyFields);
		context.write(mapKeyObj, mapValueObj);
		//不分版本
		keyFields = new String[]{appId, platform, Constants.DIMENSION_PAY_ON_VERSION, MRConstants.ALL_VERSION, accountId};
		mapKeyObj.setOutFields(keyFields);
		context.write(mapKeyObj, mapValueObj);
		
		//渠道维度付费统计
		//真实版本
		keyFields = new String[]{appId, platform, Constants.DIMENSION_PAY_ON_CHANNEL, channel, accountId};
		mapKeyObj.setOutFields(keyFields);
		context.write(mapKeyObj, mapValueObj);
		//不分版本
		keyFields = new String[]{appId, platform, Constants.DIMENSION_PAY_ON_CHANNEL, MRConstants.ALL_CHANNEL, accountId};
		mapKeyObj.setOutFields(keyFields);
		context.write(mapKeyObj, mapValueObj);
		
		//区服维度付费统计
		//真实区服
		keyFields = new String[]{appId, platform, Constants.DIMENSION_PAY_ON_GAMESERVER, gameServer, accountId};
		mapKeyObj.setOutFields(keyFields);
		context.write(mapKeyObj, mapValueObj);
		//不分区服
		keyFields = new String[]{appId, platform, Constants.DIMENSION_PAY_ON_GAMESERVER, MRConstants.ALL_GAMESERVER, accountId};
		mapKeyObj.setOutFields(keyFields);
		context.write(mapKeyObj, mapValueObj);
	}
}
