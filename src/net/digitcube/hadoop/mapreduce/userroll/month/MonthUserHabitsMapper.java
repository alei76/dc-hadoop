package net.digitcube.hadoop.mapreduce.userroll.month;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年7月31日 下午5:49:47 <br>
 * @copyrigt www.digitcube.net <br>
 * 
 * <br>
 *           输入：appid,platform,channel,gameregion,accountid,isFirstLogin,
 *           isFirstPay, monthlogintimes,
 *           monthonlinetime,monthonlineday,monthcurrencyamount,monthpaytimes
 */

public class MonthUserHabitsMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String appid = paraArr[0];
		String platform = paraArr[1];
		String channel = paraArr[2];
		String gameRegion = paraArr[3];
		boolean isFirstLogin = "1".equals(paraArr[5]);
		boolean isFirstPay = "1".equals(paraArr[6]);
		int loginTimes = StringUtil.convertInt(paraArr[7], 0);
		int onlineTime = StringUtil.convertInt(paraArr[8], 0);
		int onlineDay = StringUtil.convertInt(paraArr[9], 0);
		int currencyAmount = StringUtil.convertInt(paraArr[10], 0);
		int payTimes = StringUtil.convertInt(paraArr[11], 0);
		// A .. 统计用户平均游戏次数 游戏时长
		// appid platform channel gameregion,newuser/payuser/user‘
		OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
		OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
		if (isFirstLogin) { // 新增用户
			mapKeyObj.setOutFields(new String[] { appid, platform, channel,
					gameRegion, Constants.PLAYER_TYPE_NEWADD });
			mapValueObj.setOutFields(new String[] { "A", loginTimes + "",
					onlineTime + "" });
			context.write(mapKeyObj, mapValueObj);
		}
		if (currencyAmount > 0) { // 付费用户
			mapKeyObj.setOutFields(new String[] { appid, platform, channel,
					gameRegion, Constants.PLAYER_TYPE_PAYMENT });
			mapValueObj.setOutFields(new String[] { "A", loginTimes + "",
					onlineTime + "" });
			context.write(mapKeyObj, mapValueObj);
		}
		mapKeyObj.setOutFields(new String[] { appid, platform, channel,
				gameRegion, Constants.PLAYER_TYPE_ONLINE });
		mapValueObj.setOutFields(new String[] { "A", loginTimes + "",
				onlineTime + "" });
		context.write(mapKeyObj, mapValueObj);

		// B .. 统计月游戏天数、月游戏次数分布
		int monthOnlineDayItval = EnumConstants
				.getItval4MonthOnlineDay(onlineDay);
		int monthLoginTimesItval = EnumConstants
				.getItval4WeekLoginTimes(loginTimes);
		
		if (isFirstLogin) {
			// 月游戏天数区间
			mapKeyObj.setOutFields(new String[] { appid, platform, channel,
							gameRegion, Constants.PLAYER_TYPE_NEWADD,
							Constants.DIMENSION_PLAYER_DAYS,
							monthOnlineDayItval + "", });
			mapValueObj.setOutFields(new String[] { "B" });
			context.write(mapKeyObj, mapValueObj);
			
			// 月游戏次数区间
			mapKeyObj.setOutFields(new String[] { appid, platform, channel,
							gameRegion, Constants.PLAYER_TYPE_NEWADD,
							Constants.DIMENSION_PLAYER_LoginTimes,
							monthLoginTimesItval + "", });
			mapValueObj.setOutFields(new String[] { "B" });
			context.write(mapKeyObj, mapValueObj);
		}
		if (currencyAmount > 0) { // 付费用户
			// 月游戏天数区间
			mapKeyObj
					.setOutFields(new String[] { appid, platform, channel,
							gameRegion, Constants.PLAYER_TYPE_PAYMENT,
							Constants.DIMENSION_PLAYER_DAYS,
							monthOnlineDayItval + "", });
			mapValueObj.setOutFields(new String[] { "B" });
			context.write(mapKeyObj, mapValueObj);
			
			// 月游戏次数区间
			mapKeyObj.setOutFields(new String[] { appid, platform, channel,
					gameRegion, Constants.PLAYER_TYPE_PAYMENT,
					Constants.DIMENSION_PLAYER_LoginTimes,
					monthLoginTimesItval + "", });
			mapValueObj.setOutFields(new String[] { "B" });
			context.write(mapKeyObj, mapValueObj);
		}
		// 月游戏天数区间
		mapKeyObj.setOutFields(new String[] { appid, platform, channel,
				gameRegion, Constants.PLAYER_TYPE_ONLINE,
				Constants.DIMENSION_PLAYER_DAYS, monthOnlineDayItval + "", });
		mapValueObj.setOutFields(new String[] { "B" });
		context.write(mapKeyObj, mapValueObj);
		
		// 月游戏次数区间
		mapKeyObj.setOutFields(new String[] { appid, platform, channel,
				gameRegion, Constants.PLAYER_TYPE_ONLINE,
				Constants.DIMENSION_PLAYER_LoginTimes,
				monthLoginTimesItval + "", });
		mapValueObj.setOutFields(new String[] { "B" });
		context.write(mapKeyObj, mapValueObj);
		
		//Added at 20140809
		//付费金额及付费次数区间人数分布
		if (currencyAmount > 0) {
			//周付费次数区间人数分布
			int payTimesRange = EnumConstants.getDayPayTimesRange(payTimes);
			mapKeyObj.setOutFields(new String[] { 
					appid, 
					platform, 
					channel,
					gameRegion, 
					Constants.PLAYER_TYPE_PAYMENT,
					Constants.DIMENSION_MONTH_PayTimes, 
					payTimesRange + ""
			});
			mapValueObj.setOutFields(new String[] { "B" });
			context.write(mapKeyObj, mapValueObj);
			//周付费金额区间人数分布
			int payAmountRange = EnumConstants.getDayPayAmountRange2(currencyAmount);
			mapKeyObj.setOutFields(new String[] { 
					appid, 
					platform, 
					channel,
					gameRegion, 
					Constants.PLAYER_TYPE_PAYMENT,
					Constants.DIMENSION_MONTH_PayAmount, 
					payAmountRange + "" 
			});
			mapValueObj.setOutFields(new String[] { "B" });
			context.write(mapKeyObj, mapValueObj);
		}
	}
}
