package net.digitcube.hadoop.mapreduce.onlinenew;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.CommonExtend;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.util.IOSChannelUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * @author rickpan
 * @version 1.0
 * 
 *          输入: key : APPID, Platform, AccountID value: LoginTime, Channel,
 *          AccountType,Gender,Age,GameServer,
 *          Resolution,OperSystem,Brand,NetType,Country,Province,Operators,
 *          max(OnlineTime),max(Level)
 * 
 *          输出:<br/>
 *          APPID, Platform, AccountID, Channel,
 *          AccountType,Gender,Age,GameServer,
 *          Resolution,OperSystem,Brand,NetType,Country,Province,Operators,
 *          lastLoginTime(最后登陆时间),totalOnlineTime (总在线时长),
 *          max(Level)(最高等级),totalLoginTimes(登陆总次数)
 * 
 */

public class OnlineDayReducer
		extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private final static int Index_VERSION = 15;
	private final static int Index_Level = 14;
	private final static int Index_Onlinetime = 13;
	private final static int Index_Logintime = 0;
	private StringBuffer sb = new StringBuffer();

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		IOSChannelUtil.close();
	}

	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {

		sb.delete(0, sb.length());

		String[] keyArray = key.getOutFields();
		String pureAppId = keyArray[0];
		
		OnlineDayLog onlineDayLog = new OnlineDayLog();
		onlineDayLog.setAppID(keyArray[OnlineDayLog.INDEX_AppID]);
		onlineDayLog.setPlatform(keyArray[OnlineDayLog.INDEX_Platform]);
		onlineDayLog.setAccountID(keyArray[OnlineDayLog.INDEX_AccountID]);

		// 计算用户最高等级，最大在线时间
		// Map<String, Integer> map = new HashMap<String, Integer>();
		// 改为用 TreeMap，使得登录时间按自然排序
		TreeMap<String, Integer> map = new TreeMap<String, Integer>();
		int maxLevel = 0;
		String[] defaultValue = null;
		String UID = ""; // iOS 渠道修正用
		String maxVersion = "";// 20140723:对同一个玩家不同版本信息进行揉合，取最大版本号
		for (OutFieldsBaseModel val : values) {
			String[] value = val.getOutFields();
			String currentVerion = value[Index_VERSION];
			if (maxVersion.compareTo(currentVerion) < 0) {
				maxVersion = currentVerion;
			}
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

			// 渠道、区服等信息取最后一次的登录结果
			// defaultValue = value;
			if (loginTime.compareTo(map.lastKey()) >= 0) {
				defaultValue = value;
				//UID = val.getSuffix();
				UID = value[value.length -1];
			}
		}
		// 设置最大版本号
		onlineDayLog.setAppID(onlineDayLog.getAppID() + "|" + maxVersion);
		onlineDayLog.setExtend(new CommonExtend(defaultValue, -4));
		// 用户当天登陆总次数
		int totalLoginTimes = map.size();
		// 用户当天总在线时长
		int totalOnlineTime = 0;
		// 最后登陆时间，用户信息滚存需要这个信息
		int lastLoginTime = 0;

		/*
		 * 20140429：在玩家生命轨迹中需要记录玩家所有在线情况：登录时间、次数、时长等
		 * 对于单机型游戏，可能会在本地累积很多在线记录（如好几天），某天联网时再把所有记录上报上来 所以这里把个数限制取消
		 * 
		 * //一天内玩家的登录及在线时长最多保存 100 个记录 //如果当天登录次数小于 100，则全部登录记录都保存 //如果当天登录次数超过
		 * 100，则只抽取其中的一部分 int mod = (100 + map.size())/100; int count = 0; for
		 * (Entry<String, Integer> entry : map.entrySet()) { totalOnlineTime +=
		 * entry.getValue(); int logintime =
		 * StringUtil.convertInt(entry.getKey(), 0); lastLoginTime =
		 * lastLoginTime > logintime ? lastLoginTime : logintime;
		 * 
		 * if(0 == count%mod){
		 * sb.append(entry.getKey()).append(":").append(entry
		 * .getValue()).append(","); } count++; }
		 */
		for (Entry<String, Integer> entry : map.entrySet()) {
			totalOnlineTime += entry.getValue();
			int logintime = StringUtil.convertInt(entry.getKey(), 0);
			lastLoginTime = lastLoginTime > logintime ? lastLoginTime
					: logintime;

			// 记录所有在线情况
			sb.append(entry.getKey()).append(":").append(entry.getValue())
					.append(",");
		}

		onlineDayLog.setLastLoginTime(lastLoginTime);// 最后登陆时间
		onlineDayLog.setTotalOnlineTime(totalOnlineTime);// 总在线时长
		onlineDayLog.setMaxLevel(maxLevel);// 总在线时长
		onlineDayLog.setTotalLoginTimes(totalLoginTimes);// 总登陆次数

		// 登录及该次登录的在线时长
		// 当所有登录时间不是在当天时，用 '-' 代替
		String onlineRecords = sb.length() > 0 ? sb.substring(0,
				sb.length() - 1) : "-";
		onlineDayLog.setOnlineRecords(onlineRecords);

		// Added at 20140606 : iOS 渠道修正
		//20141202 : 对某个特定的渠道，除 iOS 外，其它平台也需进行渠道匹配
		//if (MRConstants.PLATFORM_iOS_STR.equals(onlineDayLog.getPlatform())) {
			String reviseChannel = IOSChannelUtil.checkForiOSChannel(
					pureAppId, UID, onlineDayLog.getPlatform(),
					onlineDayLog.getExtend().getChannel());
			onlineDayLog.getExtend().setChannel(reviseChannel);
		//}

		// 增加UID输出
		onlineDayLog.setUid(UID);
		
		// 20141025 兼容增加 sim 卡字段处理
		//关于运营商字段
		//新 SDK 有上报 SIM 运营商, 在 OnlineHour 中和上报运营商合到一起: 上报运营商_DC_SEP_SIM卡运营商
		//旧 SDK 没上报 SIM 运营商, 在 OnlineHour 中只有上报运营商：上报运营商
		//当没有 SIM 卡运营商时不用做特殊处理
		String[] operators = onlineDayLog.getExtend().getOperators().split(MRConstants.DC_SEPARATOR);
		if(operators.length > 1){
			onlineDayLog.getExtend().setOperators(operators[0]);//上报运营商
			onlineDayLog.setSimCradOp(operators[1]);//SIM 卡运营商
		}
		
		key.setSuffix(Constants.SUFFIX_ONLINE_DAY);
		key.setOutFields(onlineDayLog.toStringArray());
		context.write(key, NullWritable.get());
	}
}
