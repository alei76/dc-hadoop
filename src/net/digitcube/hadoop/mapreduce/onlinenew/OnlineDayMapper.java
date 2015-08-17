package net.digitcube.hadoop.mapreduce.onlinenew;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 计算当天活跃<br>
 * 
 * @author rickpan
 * @version 1.0 <br>
 *          主要逻辑： 以每小时每个玩家同一次登录的去重结果作为输入 计算每个玩家在这一天里的 登录次数、总登录时长、最高级别
 * 
 *          输入：24 个时间片 玩家每个时间片同一次登录的去重结果(@OnlineHourMapper,@OnlineHourReducer)
 *          (一条完整的在线记录，其中在线时长和级别都是去重后取最大值)
 *          Timestamp,APPID,UID,AccountID,Platform,
 *          Channel,AccountType,Gender,Age
 *          ,GameServer,Resolution,OperSystem,Brand
 *          ,NetType,Country,Province,Operators,
 *          LoginTime,max(OnlineTime),max(Level)
 * 
 *          输出： key : APPID, Platform, AccountID value: LoginTime, Channel,
 *          AccountType,Gender,Age,GameServer,
 *          Resolution,OperSystem,Brand,NetType,Country,Province,Operators,
 *          max(OnlineTime),max(Level)
 * 
 */

public class OnlineDayMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] onlineArr = value.toString().split(MRConstants.SEPERATOR_IN);
		if(onlineArr.length < 20){
			return;
		}
		String appId = onlineArr[Constants.INDEX_APPID];
		String platform = onlineArr[Constants.INDEX_PLATFORM];
		String accountId = onlineArr[Constants.INDEX_ACCOUNTID];
		String uid = onlineArr[Constants.INDEX_UID];
		String loginTime = onlineArr[Constants.INDEX_LOGIN_TIME];
		String channel = onlineArr[Constants.INDEX_CHANNEL];
		String accountType = onlineArr[Constants.INDEX_ACCOUNTTYPE];
		String gender = onlineArr[Constants.INDEX_GENDER];
		String age = onlineArr[Constants.INDEX_AGE];
		String gameServer = onlineArr[Constants.INDEX_GAMESERVER];

		String resolution = onlineArr[Constants.INDEX_RESOLUTION];
		String opSystem = onlineArr[Constants.INDEX_OPERSYSTEM];
		String brand = onlineArr[Constants.INDEX_BRAND];
		String netType = onlineArr[Constants.INDEX_NETTYPE];
		String country = onlineArr[Constants.INDEX_COUNTRY];
		String province = onlineArr[Constants.INDEX_PROVINCE];
		String operators = onlineArr[Constants.INDEX_OPERATORS];

		String onlineTime = onlineArr[Constants.INDEX_ONLINE_TIME];
		String level = onlineArr[Constants.INDEX_ONLINE_LEVEL];

		// 20140723: appId 中版本号，reduce 端处理时去取大版本号，其它信息揉合
		String[] appInfo = appId.split("\\|");
		if(appInfo.length < 2){
			return;
		}
		appId = appInfo[0];
		String appVersion = appInfo[1];

		String[] keyFields = new String[] { appId, platform, accountId,
				gameServer };
		String[] valFields = new String[] { loginTime, channel, accountType,
				gender, age, gameServer, resolution, opSystem, brand, netType,
				country, province, operators, onlineTime, level, appVersion,
				uid };

		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(valFields);

		// Added at 20140606 : 为了在 reduce 中能够用 appId + UID 进行渠道修正而添加
		/*
		 * UID 直接在 valFields 的最后一个字段中传递过去
		if (MRConstants.PLATFORM_iOS_STR.equals(platform)) {
			String UID = onlineArr[2];
			mapValObj.setSuffix(UID);
		} else {
			mapValObj.setSuffix("");
		}*/

		context.write(mapKeyObj, mapValObj);
	}
}
