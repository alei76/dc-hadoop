package net.digitcube.hadoop.mapreduce.userroll;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.model.UserInfoLog;
import net.digitcube.hadoop.util.AppCleanData;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

/**
 * 用户信息滚存，需要滚存的信息如
 * 
 * <p>
 * APPID,PlatForm,AccountID,ActTime,RegTime,FirstLoginDate,LastLoginDate(
 * 最后一次登录时间) ,TotalLoginTimes,TotalOnlineTime
 * ,TotalCurrencyAmount,TotalPayTimes(总付费次数),Level
 * </p>
 * 
 * 需要用到的日志有
 * 
 * <p>
 * *ACT* 用户激活日志 <br>
 * *REG* 用户注册日志 <br>
 * *ONLINE_DAY* 活跃用户日志 <br>
 * *PAYMENT_DAY* 付费用户日志 <br>
 * *USERROLLING_DAY* 历史用户滚存日志 <br>
 * *NEW_PLAYER_REVISE* 新增玩家日期修正(AddedByRick: 该输入只用于新增玩家修正，不应该影响原有的逻辑)
 * 
 * <p>
 * 
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月17日 上午11:07:50 @copyrigt www.digitcube.net <br>
 * 
 *          Added by rickpan 在 Reduce 端统计:
 *          在每天滚存时检查玩家是否是首次付费，如果是则输出玩家首次付费时的信息，包括： 首付时的在线天数、时长、级别、首付金额
 * 
 *          因为要输出在线天数、游戏时长这些历史信息，所以须在滚存中统计输出 然后再用 @LayoutOnFirstPayMapper
 *          对本次输出结果进行分布统计得到最终结果
 * 
 * 
 *          Added at 2013-11-19 滚存计算中以 appId,platform,accountId 为 key 进行关联计算
 *          由于历史原因，appId 字符串里包含了 appId 及 appVersion，格式为 'appId|appVersion' 这样就导致
 *          App 版本升级时，同一个玩家的信息无法再关联成功 所以这里需要把 appId 字符串拆分为纯 appId 及 appVersion
 *          为了与兼容依赖于滚存的 MR，这里 map 端拆分后，reduce 端还需合并回来
 */

public class UserInfoRollingDayMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	public final static int Index_Appid = 0;
	public final static int Index_Platform = 1;
	public final static int Index_Accountid = 2;

	public final static int Index_Rolling_ActTime = 3;
	public final static int Index_Rolling_RegTime = 4;
	public final static int Index_Rolling_FirstLoginDate = 5;
	public final static int Index_Rolling_LastLoginDate = 6;
	public final static int Index_Rolling_TotalLoginTimes = 7;
	public final static int Index_Rolling_TotalOnlineTime = 8;
	public final static int Index_Rolling_TotalCurrencyAmount = 9;
	public final static int Index_Rolling_TotalPayTimes = 10;
	public final static int Index_Rolling_Level = 11;

	public final static int Index_Payment_CurrencyAmount = 16;
	public final static int Index_Payment_TotalPayTimes = 17;

	public final static int Index_Lastlogintime = 12;
	public final static int Index_TotalOnlineTime = 13;
	public final static int Index_MaxLevel = 14;
	public final static int Index_TotalLoginTimes = 15;

	// 加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = null;

	// 当前输入的文件后缀
	private String fileSuffix = "";

	// 滚存中过滤掉某个日期前的测试数据
	// appId:filterDate
	// private Map<String, String> dateFilterMap = new HashMap<String,
	// String>();
	// private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

	// 当天需要清除的 app 的数据
	// 需要清除的 app id 由前台和调度器协调控制，滚存中不再做逻辑控制
//	private Map<String, Integer> appAndDeleteTimeMap = new HashMap<String, Integer>();
	private Map<String, Map<String,Set<String>>> appAndDeleteTimeMap = new HashMap<String, Map<String,Set<String>>>();

	// 统计的数据时间
	private int statTime = 0;
	// 所有渠道
	final String ALLCHANNEL = "ALLCHANNEL";

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(),
				new Date());

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(scheduleTime);
		calendar.add(Calendar.DAY_OF_MONTH, -1);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		statTime = (int) (calendar.getTimeInMillis() / 1000);

		// dc.filterout.appids 存在数据库任务实例的 otherArgs 中
		// 在数据库中的格式为：
		// appid_platform_deleteTime|appid_platform_deleteTime|...|appid_platform_deleteTime
		//
		// 上面的格式更新为如下，可以指定渠道删除数据其中 ALLCHANNEL 标识所有渠道!
		// appID_platform_time1:ALLCHANNEL|appID_platform_time2:ALLCHANNEL&time3:CH1+CH2+CH3
		String filteroutAppids = context.getConfiguration().get(
				"dc.filterout.appids");
		// 将原始字符串转换为map，便于后面处理
		parseToMap(filteroutAppids);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);

		String[] keyFields = null;
		String[] valueFields = null;
		String valueSuffix = "";
		if (fileSuffix.contains(Constants.SUFFIX_USERROLLING)) { // 用户滚存日志

			// UserInfoRollingLog userInfoRollingLog = new
			// UserInfoRollingLog(paraArr);
			// //加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
			UserInfoRollingLog userInfoRollingLog = new UserInfoRollingLog(
					scheduleTime, paraArr);
			
			//验证appId长度 并修正appId  add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(userInfoRollingLog.getAppID())){
				return;
			}

			/*
			 * keyFields = new String[] { userInfoRollingLog.getAppID(),
			 * userInfoRollingLog.getPlatform(),
			 * userInfoRollingLog.getAccountID() }; valueFields = new String[] {
			 * "U", userInfoRollingLog.getInfoBase64() };
			 */

			String[] appInfo = userInfoRollingLog.getAppID().split("\\|");

			// 是否是待删除的测试数据
			if (isTestData(appInfo[0], userInfoRollingLog.getPlatform(), userInfoRollingLog.getPlayerDayInfo().getChannel(),
					userInfoRollingLog)) {
				return;
			}

			keyFields = new String[] {
					appInfo[0], // 纯 appId，不带版本号
					userInfoRollingLog.getPlatform(),
					userInfoRollingLog.getAccountID(),
					userInfoRollingLog.getPlayerDayInfo().getGameRegion() };
			valueFields = new String[] { "U",
					userInfoRollingLog.getInfoBase64(), appInfo[1] // 版本号
			};
			valueSuffix = "U";
			/*
			 * } else if (fileSuffix.contains(Constants.SUFFIX_REG) ||
			 * fileSuffix.contains(Constants.SUFFIX_ACT)) { // 用户激活或注册日志
			 */
		} else if (fileSuffix.contains(Constants.SUFFIX_USERINFO_DAY)) { // 用户激活或注册日志
			UserInfoLog userInfoLog = new UserInfoLog(paraArr);
			
			String accountId = userInfoLog.getAccountID();
			// 当 appId, platform, gameServer 都相同，激活用户名为 '-'时
			// 同一个 app 同一平台、区服下的设备激活统计就会覆盖
			// 所以要用一个唯一的符号来代替激活时用户名为 '-'的用户名
			// 该用户名只用于设备激活统计使用，没有其它业务意义
			// 这里使用原有的 UID 代替 '-' 的用户名
			if (MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(accountId)) {
				accountId = "UID_" + userInfoLog.getUID();
			}
			String[] appInfo = userInfoLog.getAppID().split("\\|");

			// 是否是待删除的测试数据
			if (isTestData(appInfo[0], userInfoLog.getPlatform(), userInfoLog.getChannel(),null)) {
				return;
			}

			keyFields = new String[] { appInfo[0], userInfoLog.getPlatform(),
					accountId, userInfoLog.getGameServer() };
			valueFields = new String[] { "R", userInfoLog.getActTime() + "",
					userInfoLog.getRegTime() + "", userInfoLog.getChannel(),
					userInfoLog.getGameServer(),
					userInfoLog.getAccountNum() + "", userInfoLog.getUID(),
					appInfo[1] // appVersion
			};
			valueSuffix = "R";
		} else if (fileSuffix.contains(Constants.SUFFIX_PAYMENT_DAY)) { // 付费用户日志
			// TODO:add by seon
			if (value.getLength() > 65536) { // >64KB
				return;
			}
			PaymentDayLog paymentDayLog = new PaymentDayLog(paraArr);			
			/*
			 * keyFields = new String[] { paymentDayLog.getAppID(),
			 * paymentDayLog.getPlatform(), paymentDayLog.getAccountID() };
			 * valueFields = new String[] { "P",
			 * paymentDayLog.getCurrencyAmount() + "",
			 * paymentDayLog.getTotalPayTimes() + "",
			 * paymentDayLog.getPayRecords() //added by Rick 当天付费记录
			 * {payTime1:level1...} };
			 */

			String[] appInfo = paymentDayLog.getAppID().split("\\|");
			// 是否是待删除的测试数据
			if (isTestData(appInfo[0], paymentDayLog.getPlatform(), paymentDayLog.getExtend().getChannel(), null)) {
				return;
			}

			keyFields = new String[] { appInfo[0], paymentDayLog.getPlatform(),
					paymentDayLog.getAccountID(),
					paymentDayLog.getExtend().getGameServer() };
			/*valueFields = new String[] {
					"P",
					paymentDayLog.getCurrencyAmount() + "",
					paymentDayLog.getTotalPayTimes() + "",
					appInfo[1], // appVersion
					paymentDayLog.getExtend().getChannel(),
					paymentDayLog.getExtend().getGameServer(),
					paymentDayLog.getPayRecords() // added by Rick 当天付费记录 {//
													// payTime:currencyAmount:level}
			};*/
			valueFields = paraArr;
			valueSuffix = "P";
		} else if (fileSuffix.contains(Constants.SUFFIX_ONLINE_DAY)) { // 活跃用户日志
			// OnlineDayLog 中记录了玩家一天所有登录状态
			// 在异常情况下有玩家登录很多，到时单个 field 数值过大 UTF 写失败
			// 之前做了限制，每玩家只取 100 次登录，后来放开了，导致异常情况出现
			if (value.getLength() > 65536) { // >64KB
				return;
			}

			OnlineDayLog onlineDayLog = new OnlineDayLog(paraArr);			
			String[] appInfo = onlineDayLog.getAppID().split("\\|");
			// 是否是待删除的测试数据
			if (isTestData(appInfo[0], onlineDayLog.getPlatform(), onlineDayLog.getExtend().getChannel(), null)) {
				return;
			}
			keyFields = new String[] { appInfo[0], onlineDayLog.getPlatform(),
					onlineDayLog.getAccountID(),
					onlineDayLog.getExtend().getGameServer() };
			/*
			 * 因为新增活跃付费玩家的分布统计也放到滚存中输出 所以玩家在线信息需全量输出，才能统计各维度分布 valueFields = new
			 * String[] { "O", onlineDayLog.getLastLoginTime() + "",
			 * onlineDayLog.getTotalOnlineTime() + "",
			 * onlineDayLog.getMaxLevel() + "",
			 * onlineDayLog.getTotalLoginTimes() + "",
			 * onlineDayLog.getExtend().getChannel(),
			 * onlineDayLog.getExtend().getGameServer(), appInfo[1],
			 * onlineDayLog.getOnlineRecords() //added by Rick 当天在线记录
			 * {loginTime1:onlineTime1...} };
			 */
			valueFields = paraArr;
			valueSuffix = "O";
		} else if (fileSuffix.contains(Constants.SUFFIX_NEW_PLAYER_REVISE)) { // 新增玩家日期修正
			// 该输入只用于修正使用，不应该影响原有的输入(AddedAt:20140103)
			OnlineDayLog onlineDayLog = new OnlineDayLog(paraArr);			
			String[] appInfo = onlineDayLog.getAppID().split("\\|");
			keyFields = new String[] { appInfo[0], onlineDayLog.getPlatform(),
					onlineDayLog.getAccountID(),
					onlineDayLog.getExtend().getGameServer() };

			valueFields = paraArr;
			valueSuffix = "RV";
		}
		if (keyFields == null) {
			return;
		}
		mapKeyObj.setOutFields(keyFields);
		mapValueObj.setOutFields(valueFields);
		mapValueObj.setSuffix(valueSuffix);
		context.write(mapKeyObj, mapValueObj);
	}

	/**
	 * <pre>
	 * 判断是否为测试数据
	 */
	private boolean isTestData(String appId, String platform, String channel,
			UserInfoRollingLog userInfoRollingLog) {
		String key = appId + "_" + platform;
		// Integer deleteTime = appAndDeleteTimeMap.get(key);
		Map<String, Set<String>> deleteTimeMap = appAndDeleteTimeMap.get(key);
		if (null == deleteTimeMap) {// app 没有测试数据需要删除
			return false;
		}

		// 这里在遍历检查的过程中，一旦满足条件就可以直接返回
		for (Entry<String, Set<String>> entry : deleteTimeMap.entrySet()) {
			int deleteTime = StringUtil.convertInt(entry.getKey(), -1);
			if (deleteTime != -1) { // 删除时间有效，next
				Set<String> channelSet = entry.getValue();
				// 如果待删除的渠道集合中包含ALLCHANNEL或者指定渠道
				if (channelSet.contains(ALLCHANNEL) || channelSet.contains(channel)) {
					// deleteTime 是删除测试数据的时间点
					// [historyTime, deleteTime) 的数据都要删除
					// [deleteTime, now) 的数据都要保留
					// 当 statTime >= deleteTime 时，只删除滚存里新增日期在 deleteTime 之前的测试数据，而在线、付费等都保存
					// 当 statTime < deleteTime 时，所有数据都删除，包括滚存、在线和付费，这种情况一般在重跑的时候出现
					if (statTime >= deleteTime) {// statTime >= deleteTime
						// 只清除滚存中 FirstLoginDate 在 deleteTime 之前的玩家
						if (null != userInfoRollingLog
								&& userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate() < deleteTime) {
							System.out.println("-------------------->isTestData:true,scope:UserInfoRolling");
							System.out.println("-------------------->appId:" + appId + ",platform:" + platform
									+ ",deleteTime:" + deleteTime + ",statTime:" + statTime + ",cleanChannelSet:"
									+ channelSet + ",currentChannel:" + channel + ",accountID:"
									+ userInfoRollingLog.getAccountID() + "firstLoginTime:"
									+ userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate() + "]");
							return true;
						}

						// 非滚存数据或滚存中 FirstLoginDate 在 deleteTime 之后，则保留
						// return false;

					} else {// statTime < deleteTime

						// 所有数据都删除，包括滚存、激活、在线和付费等等
						System.out.println("-------------------->isTestData:true,scope:UserInfoRolling+Online+Payment");
						System.out.println("-------------------->appId:" + appId + ",platform:" + platform
								+ ",deleteTime:" + deleteTime + ",statTime:" + statTime + ",cleanChannelSet:"
								+ channelSet + ",currentChannel:" + channel + "]");
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 * <pre>
	 * 将字符串转换为map格式
	 * @author sam.xie
	 * @date 2015年5月18日 下午5:41:35
	 * @param filteroutAppids
	 */
	private void parseToMap(String filteroutAppids){
		/**
		 * <pre>
		 * 兼容新旧两种格式 
		 * 原来的结构：不支持渠道删选
		 * appID1_platform1_deleteTime1|appID2_platform2_deleteTime2
		 * 更新后结构：支持不同时间，不同渠道的筛选，采用json格式保存方便扩展，channels数组中如果包含ALLCHANNEL，表示所有渠道
		 * [
		 * 	{"appID":"AAAAA804765BEE789FF0CDCD2B6CC839","platform":"2","deleteTime":"1430496000","channels":["渠道1","渠道2"]},
		 * 	{"appID":"AAAAA804765BEE789FF0CDCD2B6CC839","platform":"1","deleteTime":"1430586000","channels":["ALLCHANNEL"]}
		 * ]
		 */
		if (null != filteroutAppids) {
			if (!filteroutAppids.startsWith("[")) { // 旧的格式
				String[] arrApps = filteroutAppids.split("\\|");
				for (String app : arrApps) {
					System.out.println("-------------------->Old Version AppFilter:" + app);
					String[] arr = app.split("_");
					String appAndPlatform = arr[0] + "_" + arr[1];
					String deleteTime = arr[2];
					Map<String, Set<String>> deleteMap = appAndDeleteTimeMap.get(appAndPlatform);
					if (null == deleteMap) {
						deleteMap = new HashMap<String, Set<String>>();
						Set<String> channelSet = deleteMap.get(deleteTime);
						if (null == channelSet) {
							channelSet = new HashSet<String>();
							channelSet.add(ALLCHANNEL);
							deleteMap.put(deleteTime, channelSet);
						}
					}
					appAndDeleteTimeMap.put(appAndPlatform, deleteMap);
				}
			} else { // 新的格式
				List<AppCleanData> appList = new ArrayList<AppCleanData>();
				parseToAppCleanData(filteroutAppids, appList);
				if (null != appList && appList.size() > 0) {
					for (AppCleanData data : appList) {
						System.out.println("-------------------->New Version AppFilter:" + data);
						String key = data.getAppID() + "_" + data.getPlatform();
						String deleteTime = data.getDeleteTime();
						Set<String> channelSet = data.getChannels();
						Map<String, Set<String>> deleteMap = appAndDeleteTimeMap.get(key);
						if (null == deleteMap) {
							deleteMap = new HashMap<String, Set<String>>();
							deleteMap.put(deleteTime, channelSet);
							appAndDeleteTimeMap.put(key, deleteMap);
						} else {
							Set<String> channelOldSet = deleteMap.get(deleteTime);
							if (null == channelOldSet) {
								channelOldSet = new HashSet<String>(channelSet);
							} else {
								channelOldSet.addAll(channelSet);
							}
							deleteMap.put(deleteTime, channelOldSet); // 如果已经存在就追加
						}
					}
				}
			}
		}		
	}
	
	public void parseToAppCleanData(String filteroutAppids, List<AppCleanData> list) {
		try {
			JsonElement allAppEle = StringUtil.jsonParser.parse(filteroutAppids);
			if (allAppEle.isJsonArray()) {
				JsonArray array = allAppEle.getAsJsonArray();
				Iterator<JsonElement> iterator = array.iterator();
				while (iterator.hasNext()) {
					JsonElement appEle = (JsonElement) iterator.next();
					AppCleanData data = StringUtil.gson.fromJson(appEle, AppCleanData.class);
					list.add(data);
				}
			}
		} catch (Exception e) {
			System.out.println("Parse Json Error, FilteroutApppIds:" + filteroutAppids);
			e.printStackTrace();
		}
	}
}
