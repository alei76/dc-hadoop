package test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.jce.PlayerDayInfo;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.util.AppCleanData;
import net.digitcube.hadoop.util.StringUtil;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

public class TestClearData {

	private Map<String, Map<String, Set<String>>> appAndDeleteTimeMap = new HashMap<String, Map<String, Set<String>>>();
	public static final String ALLCHANNEL = "ALLCHANNEL";
	public static int statTime = 1430496000 + 3600 * 24 * 2;

	public static void main(String[] args) throws ParseException {
		TestClearData clear = new TestClearData();
		 clear.doTest();
		// doTest();
//		clear.resolve(null);
	}

	public void doTest() throws ParseException {
		TestClearData test = new TestClearData();
		test.setup();
		// String str ="00007E2ECD0561179A60BACA19E0AC86|1.0	2	023AC5B69FF440A88E8634028E64001F	H4sIAAAAAAAAAGMK1f9-SghEKIXq32kwAhEOjAGMCZ41PXPW7Dlz580f_j8CnwRB4p-EQOQHYa4PIox_RL-JsXq6GBgYfhNnj3f08Yl3D_4kwcjAwPBJEkR-kPL4KcvAyMUE0iLgqcBowOXAyP1JDmjTiW_yCgZGxo7Opk5mlm5uJiYGjhYWrhZmxiYGRhauZiZAM92-KbA92bH26ey93xQ5n86e_XT73OdzGv8o_VH-psJo8E1V6Nma5U97Zz3Zv9A_IDjY308h2PCbGquJnpGe0Td1DnMjgwpDIwuDbxo8EEOeT9kKVPpBk-mDFtcn7VD9HwIAPBdSCPcAAAA.";
		String str = "CCCCC804765BEE789FF0CDCD2B6CC839|1.0	2	ACCOUNTid	H4sIAAAAAAAAAOMRCql9xKAEIoxAhANjAHMio0dNz5w1e87cefOH_4_AH8E_Qn-E_4j8Ef0mJhjs4h3iGhwS7-zh6Ofn6uPp8k2cPd7RxyfePfiPxB_JP1I_ZRkYuZhARgkyeigw2_hwf5ILqTt155u8gqmxsYuJgZuTm7mzubOrhZuZs4uRs6mhqaWLiZGbo7nLNwW2JzvWPp2995si59Od-5_smPN8TuMfpT_K31QYDb6pCniEONcEp-YVJ5Zk5ucpRPh8U2M10TPSM_qmzm5iYVBhYWDwTYMHYsLzKVuf7F_4QZPpj9YfbQAvcIJZ5AAAAA..";
		Date scheduleDate = StringUtil.yyyy_MM_ddHHmmss2Date("2015-02-03 06:06:06");
		UserInfoRollingLog rollingLog = new UserInfoRollingLog(scheduleDate, str.split("\t"));
		// statTime = (int)scheduleDate.getTime()/1000;
		String appId = "CCCCC804765BEE789FF0CDCD2B6CC839";
		String platform = "2";
		String channel = "channel-2";
		rollingLog.setAppID(appId);
		rollingLog.setPlatform(platform);
		PlayerDayInfo player = rollingLog.getPlayerDayInfo();
		player.setChannel(channel);
		System.out.println(test.isTestData(appId, platform, channel, rollingLog));
	}

	public void setup() {
		// String filteroutAppids =
		// "AAAAA804765BEE789FF0CDCD2B6CC839_2_1430582400:ALLCHANNEL|CCCCC804765BEE789FF0CDCD2B6CC839_2_1430496000:channel-2|CCCCC804765BEE789FF0CDCD2B6CC839_1_1430496000:channel-4+channel-2&1430582400:ALLCHANNEL";
		String filteroutAppids = "[{\"appID\":\"CCCCC804765BEE789FF0CDCD2B6CC839\",\"platform\":\"2\",\"deleteTime\":\"1430582400\",\"channels\":[\"channel-2\"]},{\"appID\":\"AAAAA804765BEE789FF0CDCD2B6CC839\",\"platform\":\"1\",\"deleteTime\":\"1430582400\",\"channels\":[\"ALLCHANNEL\"]},{\"appID\":\"CCCCC804765BEE789FF0CDCD2B6CC839\",\"platform\":\"2\",\"deleteTime\":\"1430496000\",\"channels\":[\"channel-4\"]},{\"appID\":\"AAAAA804765BEE789FF0CDCD2B6CC839\",\"platform\":\"2\",\"deleteTime\":\"1430582400\",\"channels\":[\"ALLCHANNEL\"]}]";
//		 String filteroutAppids = "CCCCC804765BEE789FF0CDCD2B6CC839_2_1429459200";
		convertToMap(filteroutAppids);
	}

	private void convertToMap(String filteroutAppids){
		/**
		 * 兼容新旧两种格式 原来的结构：不支持渠道删选 appID1_platform1_deleteTime1|appID2_platform2_deleteTime2
		 * 更新后结构：支持不同时间，不同渠道的筛选，采用json格式保存方便扩展，channels数组中如果包含ALLCHANNEL，表示所有渠道 [
		 * {"appID":"AAAAA804765BEE789FF0CDCD2B6CC839"
		 * ,"platform":"2","deleteTime":"1430496000","channels":["渠道1","渠道2"]},
		 * {"appID":"AAAAA804765BEE789FF0CDCD2B6CC839"
		 * ,"platform":"1","deleteTime":"1430586000","channels":["ALLCHANNEL"]} ]
		 */
		if (null != filteroutAppids) {
			if (!filteroutAppids.startsWith("[")) { // 旧的格式
				// for (String app : arrApps) {
				// String[] arr = app.split("_");
				// String appAndPlatform = arr[0] + "_" + arr[1];
				// int deleteTime = StringUtil.convertInt(arr[2], 0);
				// Integer anotherDelTime = appAndDeleteTimeMap
				// .get(appAndPlatform);
				// if (null == anotherDelTime) {
				// appAndDeleteTimeMap.put(appAndPlatform, deleteTime);
				// } else {
				// appAndDeleteTimeMap.put(appAndPlatform,
				// Math.max(deleteTime, anotherDelTime));
				// }
				// }
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
				resolveToAppCleanData(filteroutAppids, appList);
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
	
	private boolean isTestData(String appId, String platform, String channel, UserInfoRollingLog userInfoRollingLog) {
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
				// 如果待删除的渠道集合中包含ALLCHANNEL或者用户渠道
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

	public void resolveToAppCleanData(String filteroutAppids, List<AppCleanData> list) {
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
			System.out.println("Resolve Json Error, FilteroutApppIds:" + filteroutAppids);
			e.printStackTrace();
		}
	}
}
