package net.digitcube.hadoop.mapreduce.habits;

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
 * @version 1.0 2013年7月22日 下午8:26:58 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class UserHabitsMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String appid = paraArr[0];
		String platform = paraArr[1];
		String channel = paraArr[2];
		String gameRegion = paraArr[3];
		String userType = paraArr[4];
		int loginTimes = StringUtil.convertInt(paraArr[5], 0);
		int onlineTime = StringUtil.convertInt(paraArr[6], 0);
		int onlineDay = StringUtil.convertInt(paraArr[7], 0);
		//等级默认用 1 级
		int level = StringUtil.convertInt(paraArr[8], 0);
		level = 0 == level ? 1 : level;
		
		// A .. 统计用户平均游戏次数 游戏时长
		// appid platform channel gameregion,newuser/payuser/user
		OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel(new String[] {
				appid, platform, channel, gameRegion, userType });
		OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();

		mapValueObj.setOutFields(new String[] { "A", loginTimes + "", onlineTime + "" });
		context.write(mapKeyObj, mapValueObj);

		// B .. 统计已玩天数分布
		int onlineDayItval = EnumConstants.getItval4OnlineDays(onlineDay);
		mapKeyObj.setOutFields(new String[] { appid, platform, channel,
				gameRegion, userType, Constants.DIMENSION_PLAYER_DAYS,
				onlineDayItval + "" });
		mapValueObj.setOutFields(new String[] { "B" });
		context.write(mapKeyObj, mapValueObj);

		// C .. 在线时长区间分布
		int onlineTimeItval = EnumConstants.getItval4OnlineTime(onlineTime);
		mapKeyObj.setOutFields(new String[] { appid, platform, channel,
				gameRegion, userType, Constants.DIMENSION_PLAYER_ONLINETIME,
				onlineTimeItval + "" });
		mapValueObj.setOutFields(new String[] { "B" });
		context.write(mapKeyObj, mapValueObj);

		// D .. 等级区间分布
		mapKeyObj.setOutFields(new String[] { appid, platform, channel,
				gameRegion, userType, Constants.DIMENSION_PLAYER_LEVEL,
				level + "" });
		mapValueObj.setOutFields(new String[] { "B" });
		context.write(mapKeyObj, mapValueObj);
		// E .. 日游戏次数分布
		int loginTimesItval = EnumConstants.getItval4DayLoginTimes(loginTimes);
		mapKeyObj.setOutFields(new String[] { appid, platform, channel,
				gameRegion, userType, Constants.DIMENSION_PLAYER_LoginTimes,
				loginTimesItval + "" });
		mapValueObj.setOutFields(new String[] { "B" });
		context.write(mapKeyObj, mapValueObj);
	}
}
