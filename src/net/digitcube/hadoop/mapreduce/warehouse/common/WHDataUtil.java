package net.digitcube.hadoop.mapreduce.warehouse.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import net.digitcube.hadoop.model.warehouse.WHPaymentLog;
import net.digitcube.hadoop.util.JdbcUtil;
import net.digitcube.hadoop.util.StringUtil;

public class WHDataUtil {
	public static Set<String> appBlacklist = null;
	public static Map<String, AppModel> appMap = null;
	public static Map<String, Float> exchangeMap = null;

	static {
		appBlacklist = JdbcUtil.getAppBlacklist();
		appMap = JdbcUtil.getAppInfoMap();
		exchangeMap = new HashMap<String, Float>();
		exchangeMap.put("CNY", 1.0000f);
		exchangeMap.put("USD", 6.2081f);
		exchangeMap.put("EUR", 6.9953f);
		exchangeMap.put("GBP", 9.6586f);
	}

	/**
	 * <pre>
	 * ********************付费规则********************
	 * 1.APPID  
	 * 	1.1	公司名包含：慧动/DataEye(大小写）/学习
	 * 	1.2	游戏名包含：测试/test/demo/体验/无意义的数字组合或字母组合
	 * 	1.3	黑名单中的App
	 *  
	 * 	2.UID
	 * 	2.1 UID 以DEUID_开头的UID
	 * 
	 * 3.付费金额：
	 * 	3.1	单笔金额>=3K RMB
	 * 	3.2	1分钟内>=10次支付
	 * 	3.3	1天总支付金额>=3K RMB and 1天总支付次数>=60次
	 * 3.4	1天总支付金额>=5W RMB
	 ***************************************************
	 */
	public static String doFilter(String appId, String uid, List<WHPaymentLog> paymentList) {
		Set<String> resultSet = new TreeSet<String>();
		AppModel app = appMap.get(appId);
		// 过滤App名称和公司名称
		if (null != app) {
			String companyName = app.getCompanyName().toLowerCase();
			String appName = app.getAppName().toLowerCase();
			String[] invalidCompany = { "慧动", "dataeye", "学习" };
			String[] invalidApp = { "测试", "test", "demo", "体验" };
			if (keywordCheck(companyName, invalidCompany)) {
				resultSet.add("1.1");
			}
			if (keywordCheck(appName, invalidApp)) {
				resultSet.add("1.2");
			}
		}
		// 过滤黑名单
		if (appBlacklist.contains(appId)) {
			resultSet.add("1.3");
		}
		// 过滤uid
		// 这条清理规则无效，不再使用
		// update 2015.07.14
		// if (uid.toLowerCase().startsWith("deuid_")) {
		// resultSet.add("2.1");
		// }
		// 过滤付费
		if (null != paymentList) {
			float totalAmount = 0f;
			int totalTimes = 0;
			// 每个时间点的付费次数
			List<Integer> payTimes = new ArrayList<Integer>();
			for (WHPaymentLog val : paymentList) {
				int payTime = val.getPayTime();
				float payAmount = getExchangeAmount(val.getCurrencyAmount(), appId);
				if (payAmount >= 3000) {
					resultSet.add("3.1");
				}
				// 汇总付费金额
				totalAmount += payAmount;
				// 统计付费频次
				payTimes.add(payTime);
				// 总次数
				totalTimes++;
			}

			// 频次判断
			if (payFrequency(payTimes)) {
				resultSet.add("3.2");
			}
			if (totalAmount >= 3000 && totalTimes >= 60) {
				resultSet.add("3.3");
			}
			if (totalAmount > 50000) {
				resultSet.add("3.4");
			}
		}
		if (resultSet.size() > 0) {// 如果匹配到被过滤的记录，就以逗号分割的方式返回被过滤的条件
			return StringUtil.join(resultSet, ",");
		} else {
			return "-";
		}
	}

	public static String keywordFilter(String appId, String uid) {
		return doFilter(appId, uid, null);
	}

	public static float getExchangeAmount(float amount, String appId) {
		Float exchangeRate = 1.0f;
		if (null != appMap.get(appId)) {
			exchangeRate = exchangeMap.get(appMap.get(appId).getCurrency());
		}
		exchangeRate = (exchangeRate == null ? 1.0f : exchangeRate);
		return amount * exchangeRate;
	}

	public static boolean payFrequency(List<Integer> payTimes) {
		Collections.sort(payTimes);
		int times = 0;
		int interval = 0;
		for (int i = 0; i < payTimes.size(); i++) {
			for (int j = i + 1; j < payTimes.size(); j++) {
				interval = payTimes.get(j) - payTimes.get(i);
				times = j - i + 1;
				if (interval <= 60 && times >= 10) {
					return true;
				}
			}
		}
		return false;
	}

	public static boolean keywordCheck(String name, String[] filterKey) {
		if (StringUtil.isEmpty(name) || null == filterKey) {
			return false;
		}
		for (String key : filterKey) {
			if (name.toLowerCase().contains(key.toLowerCase())) {
				return true;
			}
		}
		return false;
	}

	public static int markLogin(int markValue, int offset) {
		return markValue | (1 << (offset - 1));
	}

}
