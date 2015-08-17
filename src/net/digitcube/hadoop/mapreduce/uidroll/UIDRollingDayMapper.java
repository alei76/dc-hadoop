package net.digitcube.hadoop.mapreduce.uidroll;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.UIDRollingLog;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.model.UserInfoLog;
import net.digitcube.hadoop.util.AppCleanData;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

public class UIDRollingDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	
	// 统计的数据时间
	private int statTime = 0;
	
	// 当前输入的文件后缀
	private String fileSuffix;
	
	// 当天需要清除的 app 的数据
	// 需要清除的 app id 由前台和调度器协调控制，滚存中不再做逻辑控制
//	private Map<String, Integer> appAndDeleteTimeMap = new HashMap<String, Integer>();
	private Map<String, Map<String,Set<String>>> appAndDeleteTimeMap = new HashMap<String, Map<String,Set<String>>>();
	// 所有渠道
	final String ALLCHANNEL = "ALLCHANNEL";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		if(context.getConfiguration().getBoolean("is.hour.job", false)){
			//小时任务:结算时间取当天凌晨零点
			statTime = DateUtil.getStatDateForToday(context.getConfiguration());
		}else{
			//天任务:结算时间取昨天凌晨零点
			statTime = DateUtil.getStatDate(context.getConfiguration());
		}
		
		// dc.filterout.appids 存在数据库任务实例的 otherArgs 中
		// 在数据库中的格式为：[appid_platform_deleteTime...]
		String filteroutAppids = context.getConfiguration().get("dc.filterout.appids");
//		if (null != filteroutAppids) {
//			String[] arrApps = filteroutAppids.split("\\|");
//			for (String app : arrApps) {
//				String[] arr = app.split("_");
//				String appAndPlatform = arr[0] + "_" + arr[1];
//				int deleteTime = StringUtil.convertInt(arr[2], 0);
//				Integer anotherDelTime = appAndDeleteTimeMap.get(appAndPlatform);
//				if (null == anotherDelTime) {
//					appAndDeleteTimeMap.put(appAndPlatform, deleteTime);
//				} else {
//					appAndDeleteTimeMap.put(appAndPlatform,Math.max(deleteTime, anotherDelTime));
//				}
//			}
//		}
		// 将原始字符串转换为map，便于后面处理
		parseToMap(filteroutAppids);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);

		if(fileSuffix.endsWith(Constants.SUFFIX_UID_ROLLING_DAY)) { // 用户滚存日志
			UIDRollingLog uidRollingLog = new UIDRollingLog(paraArr);
			
			//验证appId长度 并修正appId  add by mikefeng 20141010
			if(StringUtil.isEmpty(uidRollingLog.getAppID()) 
					|| uidRollingLog.getAppID().length() != 32){
				return;
			}
			
			//删除用户指定的测试数据
			if(isTestData(uidRollingLog.getAppID(), uidRollingLog.getPlatform(), uidRollingLog.getChannel(), uidRollingLog)){
				return;
			}
			
			String[] keyFields = new String[] {
					uidRollingLog.getAppID(), // 纯 appId，不带版本号
					uidRollingLog.getPlatform(),
					uidRollingLog.getGameServer(),
					uidRollingLog.getUid()
			};
			
			mapKeyObj.setOutFields(keyFields);
			mapValueObj.setOutFields(paraArr);
			mapValueObj.setSuffix("R");
			context.write(mapKeyObj, mapValueObj);
			
		}else if (fileSuffix.contains("Online")) { // 活跃用户日志
																		// s
			OnlineLog onlineLog = null;
			try{
				//日志里某些字段有换行字符，导致这条日志换行而不完整，
				//用 try-catch 过滤掉这类型的错误日志
				onlineLog = new OnlineLog(paraArr);
			}catch(Exception e){
				return;
			}
			
			//20141011
			//与彭俊约定，在线日志中只要 AccountId 是 _DESelf_DEFAULT_ACCOUNTID 的数据都过滤掉
			//这个是 SDK 中的一个 BUG
			if(onlineLog.getAccountID().equals(Constants.NO_LOGIN_ACCOUNTID)){
				return;
			}
			
			//验证appId长度 并修正appId  add by mikefeng 20141101
			if(!FieldValidationUtil.validateAppIdLength(onlineLog.getAppID())){
				return;
			}
			
			String[] appInfo = onlineLog.getAppID().split("\\|");
			
			//删除用户指定的测试数据
			if(isTestData(appInfo[0], onlineLog.getPlatform(), onlineLog.getChannel(), null)){
				return;
			}	
			
			//真实区服
			String[] keyFields = new String[] { 
					appInfo[0], //纯 APPID
					onlineLog.getPlatform(),
					onlineLog.getGameServer(),
					onlineLog.getUID()
			};
			String[] valueFields = new String[] {
					appInfo[1], // 版本号
					onlineLog.getChannel(),
					onlineLog.getAccountID(),
					onlineLog.getLoginTime()+""
			};
			mapKeyObj.setOutFields(keyFields);
			mapValueObj.setOutFields(valueFields);
			mapValueObj.setSuffix("O");
			context.write(mapKeyObj, mapValueObj);
			
			//全服
			String[] keyFields_AllGS = new String[] { 
					appInfo[0], //纯 APPID
					onlineLog.getPlatform(),
					MRConstants.ALL_GAMESERVER,
					onlineLog.getUID()
			};
			mapKeyObj.setOutFields(keyFields_AllGS);
			mapValueObj.setOutFields(valueFields);
			mapValueObj.setSuffix("O");
			context.write(mapKeyObj, mapValueObj);
			
		}else if (fileSuffix.contains("UserInfo")) { // 用户注册日志
			UserInfoLog userInfoLog = null;
			try{
				//日志里某些字段有换行字符，导致这条日志换行而不完整，
				//用 try-catch 过滤掉这类型的错误日志
				userInfoLog = new UserInfoLog(paraArr);
			}catch(Exception e){
				//TODO do something to mark the error here
				return;
			}
			
			// 玩家注册登录前的数据以默认以 NO_LOGIN_ACCOUNTID 的身份上报
			// 而该玩家在 SDK 端的表现是激活和注册同时完成的，这就引起注册转化率达到 100% 的问题
			// 20141106 与彭俊约定，如果碰到是该玩家的注册日志，注册时间统统设为 0 
			if(userInfoLog.getAccountID().equals(Constants.NO_LOGIN_ACCOUNTID)){
				userInfoLog.setRegTime(0);
			}
			
			// 只有注册时间大于 0 的才计算新增设备
			if(userInfoLog.getRegTime() <= 0){
				return;
			}
			
			//验证appId长度 并修正appId  add by mikefeng 20141101
			if(!FieldValidationUtil.validateAppIdLength(userInfoLog.getAppID())){
				return;
			}
			
			String[] appInfo = userInfoLog.getAppID().split("\\|");
			
			//删除用户指定的测试数据
			if(isTestData(appInfo[0], userInfoLog.getPlatform(), userInfoLog.getChannel(), null)){
				return;
			}
			
			//真实区服
			String[] keyFields = new String[] { 
					appInfo[0], //纯 APPID
					userInfoLog.getPlatform(),
					userInfoLog.getGameServer(),
					userInfoLog.getUID()
			};
			String appVersion = "1.0";
			if(appInfo.length == 2){
				appVersion = appInfo[1];
			}
			String[] valueFields = new String[] {
					appVersion, // 版本号
					userInfoLog.getChannel(),
					userInfoLog.getAccountID(),
					userInfoLog.getRegTime()+""
			};
			mapKeyObj.setOutFields(keyFields);
			mapValueObj.setOutFields(valueFields);
			mapValueObj.setSuffix("REG");
			context.write(mapKeyObj, mapValueObj);
			
			//全服
			String[] keyFields_AllGS = new String[] { 
					appInfo[0], //纯 APPID
					userInfoLog.getPlatform(),
					MRConstants.ALL_GAMESERVER,
					userInfoLog.getUID()
			};
			mapKeyObj.setOutFields(keyFields_AllGS);
			mapValueObj.setOutFields(valueFields);
			mapValueObj.setSuffix("REG");
			context.write(mapKeyObj, mapValueObj);
		}
	}

	/**
	 * <pre>
	 * 判断是否为测试数据
	 */
	private boolean isTestData(String appId, String platform, String channel,
			UIDRollingLog uidRollingLog) {
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
						if (null != uidRollingLog
								&& uidRollingLog.getFirstLoginDate() < deleteTime) {
							System.out.println("-------------------->isTestData:true,scope:UserInfoRolling");
							System.out.println("-------------------->appId:" + appId + ",platform:" + platform
									+ ",deleteTime:" + deleteTime + ",statTime:" + statTime + ",cleanChannelSet:"
									+ channelSet + ",currentChannel:" + channel + ",accountID:"
									+ uidRollingLog.getAccountID() + "firstLoginTime:"
									+ uidRollingLog.getFirstLoginDate() + "]");
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
