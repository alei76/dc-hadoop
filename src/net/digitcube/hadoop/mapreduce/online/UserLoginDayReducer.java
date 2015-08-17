package net.digitcube.hadoop.mapreduce.online;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.CommonExtend;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月15日 下午5:05:26 @copyrigt www.digitcube.net<br/>
 *          输入：<br/>
 *          key:appid,platform,accountid<br>
 *          value:logintime,channel,accounttype,gender,age,gameserver,resolution
 *          , opersystem,brand,nettype,country,province,operators,MAXonlinetime,
 *          MAXlevel<br>
 *          输出:<br/>
 *          appid,platform,accountid,channel,accounttype,gender,age,gameserver,
 *          resolution ,
 *          opersystem,brand,nettype,country,province,operators,lastlogintime
 *          (最后登陆时间),totalOnlineTime (总在线时长), maxLevel(最高等级),,
 *          totalLoginTimes(登陆总次数)
 */

/**
 * use @PlayerOnlineDayMapper and @PlayerOnlineDayReducer instead
 */
@Deprecated
public class UserLoginDayReducer
		extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private final static int Index_Level = 14;
	private final static int Index_Onlinetime = 13;
	private final static int Index_Logintime = 0;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// do some setup before map
		super.setup(context);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		String[] keyArray = key.getOutFields();
		OnlineDayLog onlineDayLog = new OnlineDayLog();
		onlineDayLog.setAppID(keyArray[OnlineDayLog.INDEX_AppID]);
		onlineDayLog.setPlatform(keyArray[OnlineDayLog.INDEX_Platform]);
		onlineDayLog.setAccountID(keyArray[OnlineDayLog.INDEX_AccountID]);

		// 计算用户最高等级，最大在线时间
		Map<String, Integer> map = new HashMap<String, Integer>();
		int maxLevel = 0;
		String[] defaultValue = null;
		for (OutFieldsBaseModel val : values) {
			String[] value = val.getOutFields();
			int level = StringUtil.convertInt(value[Index_Level], 0);// level
			int online = StringUtil.convertInt(value[Index_Onlinetime], 0);// online
			String loginTime = value[Index_Logintime]; // logintime
			maxLevel = maxLevel > level ? maxLevel : level;
			Integer onlinetime1 = map.get(loginTime);
			if (onlinetime1 == null) {
				map.put(loginTime, online);
			} else {
				online = online > onlinetime1 ? online : onlinetime1;
				map.put(loginTime, online);
			}
			defaultValue = value;
		}
		onlineDayLog.setExtend(new CommonExtend(defaultValue, -4));
		// 用户当天登陆总次数
		int totalLoginTimes = map.size();
		// 用户当天总在线时长
		int totalOnlineTime = 0;
		// 最后登陆时间，用户信息滚存需要这个信息
		int lastLoginTime = 0;
		for (Entry<String, Integer> entry : map.entrySet()) {
			totalOnlineTime += entry.getValue();
			int logintime = StringUtil.convertInt(entry.getKey(), 0);
			lastLoginTime = lastLoginTime > logintime ? lastLoginTime
					: logintime;
		}
		onlineDayLog.setLastLoginTime(lastLoginTime);// 最后登陆时间
		onlineDayLog.setTotalOnlineTime(totalOnlineTime);// 总在线时长
		onlineDayLog.setMaxLevel(maxLevel);// 总在线时长
		onlineDayLog.setTotalLoginTimes(totalLoginTimes);// 总登陆次数

		key.setSuffix(Constants.SUFFIX_ONLINE_DAY);
		key.setOutFields(onlineDayLog.toStringArray());
		context.write(key, NullWritable.get());
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}

}
