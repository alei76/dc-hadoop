package test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.model.PaymentLog;
import net.digitcube.hadoop.model.UserInfoLog;
import net.digitcube.hadoop.model.warehouse.WHOnlineLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.xunlei.util.Log;

public class DataTask {

	public static JsonParser jsonParser = new JsonParser();

	private static Logger logger = Log.getLogger("Test");

	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
		DataTask test = new DataTask();
		// test.getXiaoMieXingXing();
		// test.getNewUID();
		// test.getActiveIDFA();
		// test.getActDevice();
		// test.getOnlineUser();
		// test.getnew();
		// test.getPayment();
		// test.getCleanData();
		// test.getActDevice();
		// test.getNewUser();
		// test.getNewUser2();
		// test.getNewEquip();
		// test.getDeviceInfoByAccountID();
		test.getHourStat();
	}

	public void getOnlineUser() throws IOException {
		String file1 = "D:\\datatask\\huoying\\online_day.0519.2";
		String file2 = "D:\\datatask\\huoying\\onlineipimei.0519.uniq";
		BufferedReader br = new BufferedReader(new FileReader(file2));
		Map<String, String> map = new HashMap<String, String>();
		String line = "";
		while (null != (line = br.readLine())) {
			String[] arr = line.split(",");
			// System.out.println(line);
			String accountID = arr[0];
			String ip = arr[1];
			String[] imeiArr = arr[2].replace("\"", "").split(":");
			try {
				String imei = "";
				if (imeiArr.length == 2) {
					imei = imeiArr[1];
				}
				String value = map.get(accountID);
				if (null == value) {
					imei = StringUtil.isEmpty(imei) ? "-" : imei;
					map.put(accountID, ip + "," + imei);
				} else {
					if (!StringUtil.isEmpty(imei)) {
						map.put(accountID, ip + "," + imei);
					}
				}
			} catch (Exception e) {
			}
		}
		br.close();
		br = new BufferedReader(new FileReader(file1));
		int i = 0;
		while (null != (line = br.readLine())) {
			OnlineDayLog log = new OnlineDayLog(line.split("\t"));
			String accountID = log.getAccountID();
			String info = map.get(accountID) + "," + accountID + "," + log.getTotalLoginTimes() + ","
					+ log.getTotalOnlineTime() + "," + log.getMaxLevel() + "," + log.getExtend().getCountry() + ","
					+ log.getExtend().getProvince() + "," + log.getExtend().getChannel();
			System.out.println(info);
			i++;
		}
		System.out.println("count:" + i);

	}

	/**
	 * <pre>
	 * 
	 * @author sam.xie
	 * @throws IOException
	 * @date 2015年5月12日 上午11:29:20
	 */
	public void getnew() throws IOException {
		String fileDate = ".0509";
		String fileDetail = "D:\\huoying\\detail";
		// String fileOnline = "D:\\huoying\\online";
		String fileUserInfo = "D:\\huoying\\userinfo";

		String newUserFile = "D:\\huoying\\newuser" + fileDate + ".csv";
		String actUserFile = "D:\\huoying\\actuser" + fileDate + ".csv";

		BufferedWriter bw1 = new BufferedWriter(new FileWriter(newUserFile));
		BufferedWriter bw2 = new BufferedWriter(new FileWriter(actUserFile));

		BufferedReader br1 = new BufferedReader(new FileReader(fileDetail + fileDate));
		// BufferedReader br2 = new BufferedReader(new FileReader(fileOnline + fileDate));
		BufferedReader br3 = new BufferedReader(new FileReader(fileUserInfo + fileDate));

		// 1.获取UID，AccountID，IMEI，注册时间，品牌
		String line = "";
		Map<String, String> actTimeMap = new HashMap<String, String>();
		Map<String, String> imeiMap = new HashMap<String, String>();
		Map<String, String> ipMap = new HashMap<String, String>();
		while (null != (line = br1.readLine())) {
			UserInfoLog log = new UserInfoLog(line.split("\t"));
			String key = log.getUID();
			if (log.getActTime() > 0) {
				actTimeMap.put(key, log.getActTime() + "");
			}
			imeiMap.put(key, log.getImei());
			ipMap.put(key, log.getExtend_4());
		}
		// 3.获取AccountID,PlayerType,
		int newCount = 0;
		int actCount = 0;
		int allCount = 0;
		String head = "注册时间,IP地址,IMEI,AccountID,登录次数,游戏时长,等级,国家,省份,品牌\n";
		bw1.append(head);
		bw2.append(head);
		while (null != (line = br3.readLine())) {
			String[] array = line.split("\t");
			String playerType = array[array.length - 1];
			String[] onlineDayArr = new String[array.length - 1];
			System.arraycopy(array, 0, onlineDayArr, 0, array.length - 1);
			OnlineDayLog log = new OnlineDayLog(onlineDayArr);
			if (!"_ALL_GS".endsWith(log.getExtend().getGameServer())) {
				continue;
			}
			String key = log.getUid();
			String value = log.getAccountID() + "\t" + log.getTotalLoginTimes() + "\t" + log.getTotalOnlineTime()
					+ "\t" + log.getMaxLevel() + "\t" + log.getExtend().getCountry() + "\t"
					+ log.getExtend().getProvince() + "\t" + log.getExtend().getBrand();
			String record = actTimeMap.get(key) + "\t" + ipMap.get(key) + "\t" + imeiMap.get(key) + "\t" + value;
			System.out.println(record);
			if (playerType.contains("N")) {
				record = record.replace("\t", ",");
				bw1.append(record + "\n");
				newCount++;

			}
			if (playerType.contains("O")) {
				record = record.replace("\t", ",");
				bw2.append(record + "\n");
				actCount++;
			}
			allCount++;
		}
		bw1.close();
		bw2.close();
		br1.close();
		br3.close();
		System.out.println("newCcount:" + newCount);
		System.out.println("actCcount:" + actCount);
		System.out.println("allCcount:" + allCount);
	}

	/**
	 * 设备激活
	 * 
	 * @throws IOException
	 */
	public void getActDevice() throws IOException {
		String path = "D:\\datatask\\huoying\\";
		String file = "userinfo.0624";
		BufferedReader reader = new BufferedReader(new FileReader(path + file));
		StringBuilder sb = new StringBuilder();
		Map<String, String> resultMap = new HashMap<String, String>();
		String line = "";
		while ((line = reader.readLine()) != null) {
			sb.delete(0, sb.length());
			UserInfoLog log = new UserInfoLog(line.split("\t"));
			// 激活时间 > 0
			if (log.getActTime() > 0 && log.getPlatform().equals("2")) {
				resultMap.put(log.getUID(), log.getActTime() + "," + log.getUID() + "," + log.getAccountID() + ","
						+ log.getChannel() + "," + log.getBrand() + "," + log.getOperators() + "," + log.getCountry()
						+ "," + log.getProvince() + "," + log.getImei() + "," + log.getMac() + "," + log.getExtend_4());
			}
		}
		reader.close();
		BufferedWriter writer = new BufferedWriter(new FileWriter(path + file + ".设备激活.csv"));
		writer.write("激活时间,UID,帐号,渠道,机型,操作系统,国家,省份,IMEI,MAC,IP\n");
		for (String value : resultMap.values()) {
			System.out.println(value);
			writer.append(value + "\n");
		}
		writer.close();
		System.out.println("-----" + resultMap.keySet().size());
	}

	/**
	 * 新增帐号
	 * 
	 * @throws IOException
	 */
	public void getNewUser() throws IOException {
		String path = "D:\\datatask\\huoying\\";
		String file = "userinfo.0624";
		BufferedReader reader = new BufferedReader(new FileReader(path + file));
		StringBuilder sb = new StringBuilder();
		Map<String, String> resultMap = new HashMap<String, String>();
		String line = "";
		while ((line = reader.readLine()) != null) {
			sb.delete(0, sb.length());
			UserInfoLog log = new UserInfoLog(line.split("\t"));
			// 注册时间 > 0
			if (log.getRegTime() > 0 && log.getPlatform().equals("2")
					&& (!"_DESelf_DEFAULT_ACCOUNTID".equals(log.getAccountID()))
					&& log.getChannel().contains("qihooandroid")) {
				resultMap.put(
						log.getAccountID(),
						log.getRegTime() + "," + log.getUID() + "," + log.getAccountID() + "," + log.getChannel() + ","
								+ log.getBrand() + "," + log.getOperSystem() + "," + log.getCountry() + ","
								+ log.getProvince() + "," + log.getImei() + "," + log.getMac() + ","
								+ log.getExtend_4());
			}
		}
		reader.close();
		BufferedWriter writer = new BufferedWriter(new FileWriter(path + file + ".新增玩家.csv"));
		writer.write("注册时间,UID,帐号,渠道,机型,操作系统,国家,省份,IMEI,MAC,IP\n");
		for (String value : resultMap.values()) {
			System.out.println(value);
			writer.append(value + "\n");
		}
		writer.close();
		System.out.println("-----" + resultMap.keySet().size());
	}

	/**
	 * 新增帐号
	 * 
	 * @throws IOException
	 */
	public void getNewUser2() throws IOException {
		String[] dates = { ".0624", ".0625", ".0626", ".0627" };
		for (String date : dates) {

			String path = "D:\\datatask\\huoying\\newuser\\";
			String file = "online.log.qihooandroid" + date;// "new_player_accountid.0624";
			BufferedReader reader = new BufferedReader(new FileReader(path + file));
			Map<String, String> resultMap = new TreeMap<String, String>();
			String line = "";
			while ((line = reader.readLine()) != null) {
				WHOnlineLog log = new WHOnlineLog(line.split("\t"));
				// 注册时间 > 0
				resultMap.put(log.getAccountId(), log.getAccountId() + "," + log.getChannel() + "," + log.getBrand()
						+ "," + log.getOs() + "," + log.getImei() + "," + log.getMac() + "," + log.getCountry() + ","
						+ log.getProvince() + log.getIp());
			}
			reader.close();

			file = "payment.log.qihooandroid" + date;
			reader = new BufferedReader(new FileReader(path + file));
			while ((line = reader.readLine()) != null) {
				PaymentLog log = new PaymentLog(line.split("\t"));
				String imei = "-";
				String mac = "-";
				if (log.getImei().split("\\|").length > 1) {
					imei = log.getImei().split("\\|")[0];
					mac = log.getImei().split("\\|")[1];
				}
				resultMap.put(
						log.getAccountID(),
						log.getAccountID() + "," + log.getChannel() + "," + log.getBrand() + "," + log.getOperSystem()
								+ "," + imei + "," + mac + "," + log.getCountry() + "," + log.getProvince() + ","
								+ log.getExtend_4());
			}
			reader.close();

			file = "new_player_accountid" + date;
			BufferedWriter writer = new BufferedWriter(new FileWriter(path + file + ".新增玩家.csv"));
			writer.write("帐号,渠道,机型,操作系统,IMEI,MAC,国家,省份,IP\n");
			reader = new BufferedReader(new FileReader(path + file));
			int counter = 0;
			while ((line = reader.readLine()) != null) {
				String detail = resultMap.get(line);
				if (null == detail) {
					detail = line + ",-,-,-,-,-,-,";
				}
				System.out.println(detail);
				writer.append(detail + "\n");
				counter++;
			}
			writer.close();
			reader.close();
			System.out.println("-----" + counter);

		}
	}

	/**
	 * 新增设备
	 */
	public void getNewEquip() throws NumberFormatException, IOException {
		String path = "D:\\datatask\\huanlechuangtianxia\\";
		String fileName = path + "userinfo.log.0625";
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		BufferedWriter writer = new BufferedWriter(new FileWriter(fileName + ".csv"));
		String line = "";
		String header = "AppID,平台(1.ios 2.android),UID,AccountID,渠道,注册时间,MAC,IMEI,国家,地区,IP";
		writer.write("");
		writer.append(header + "\n");
		System.out.println(header);
		int count = 0;
		while ((line = reader.readLine()) != null) {
			UserInfoLog log = new UserInfoLog(line.split("\t"));
			if (log.getRegTime() > 0 && log.getPlatform().equals("2")) {
				String record = merge(
						new Object[] { log.getAppID(), log.getPlatform(), log.getUID(), log.getAccountID(),
								log.getChannel(), getDateTime(log.getRegTime()), log.getMac(), log.getImei(),
								log.getCountry(), log.getProvince(), log.getExtend_4() }, ",");
				System.out.println(record);
				writer.append(record + "\n");
				count++;
			}
		}
		System.out.println("count:" + count);
		reader.close();
		writer.close();
	}

	public void getXiaoMieXingXing() throws NumberFormatException, IOException {
		Set<String> deIMEI = new HashSet<String>();
		Set<String> otherIMEI = new HashSet<String>();
		BufferedReader reader = new BufferedReader(new FileReader("D:\\datatask\\IMEI.log"));
		String line = "";
		while ((line = reader.readLine()) != null) {
			String[] arr = line.split("\t");
			if (arr.length == 5) {
				String extinfo = arr[2];
				int actTime = Integer.parseInt(arr[3]);
				String imei = convertJson2Map(extinfo).get("IMEI");
				imei = StringUtils.isBlank(imei) ? "NULL_IMEI" : imei;
				if (actTime > 0) {
					if (!deIMEI.contains(imei)) {
						deIMEI.add(imei);
					}
				}
			}
		}

		reader.close();
		reader = new BufferedReader(new FileReader("D:\\datatask\\xingxing.log"));
		while ((line = reader.readLine()) != null) {
			otherIMEI.add(line);
		}

		reader.close();

		System.out.println("DataEye IMEI：" + deIMEI.size());
		System.out.println("消灭星星  IMEI：" + otherIMEI.size());

		Set<String> commonIMEI = new HashSet<String>(otherIMEI);
		commonIMEI.retainAll(deIMEI);
		System.out.println("公共IMEI：" + commonIMEI.size());
		printSet(commonIMEI);
		deIMEI.removeAll(commonIMEI);
		System.out.println("DataEye独有IMEI：" + deIMEI.size());
		printSet(deIMEI);
		otherIMEI.removeAll(commonIMEI);
		System.out.println("消灭星星独有IMEI：" + otherIMEI.size());
		printSet(otherIMEI);
	}

	public void getNewUID() throws NumberFormatException, IOException {
		Set<String> onlineUID = new HashSet<String>();
		Set<String> regUserUID = new HashSet<String>();
		BufferedReader reader = new BufferedReader(new FileReader("D:\\datatask\\userinfo-regtime.20150421.log"));
		String line = "";
		while ((line = reader.readLine()) != null) {
			String[] arr = line.split("\t");
			if (arr.length == 4) {
				String uid = arr[0];
				// int regTime = Integer.parseInt(arr[3]);
				// if (regTime > 0) {
				regUserUID.add(uid);
				// }
			}
		}

		reader.close();
		reader = new BufferedReader(new FileReader("D:\\datatask\\online-uid.20150421.log"));
		while ((line = reader.readLine()) != null) {
			onlineUID.add(line);
		}

		reader.close();

		System.out.println("注册UID：" + regUserUID.size());
		System.out.println("在线UID：" + onlineUID.size());

		Set<String> commonIMEI = new HashSet<String>(regUserUID);
		commonIMEI.retainAll(onlineUID);
		System.out.println("新增UID：" + commonIMEI.size());
		printSet(commonIMEI);
	}

	/**
	 * <pre>
	 * 获取付费记录
	 * @author sam.xie
	 * @date 2015年6月3日 下午4:57:01
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public void getPayment() throws NumberFormatException, IOException {
		String[] dateArr = { ".0708", ".0709", ".0710", ".0711", ".0712", ".0713", ".0714" };
		for (String date : dateArr) {
			String path = "D:\\datatask\\woshisishen_1167\\";
			String fileName = path + "log_payment_detail" + date;
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			BufferedWriter writer = new BufferedWriter(new FileWriter(fileName + ".csv"));
			String line = "";
			String header = "AccountID,AppID,平台(1.ios 2.android 3.win),渠道,IDFA,付费时间,订单号,付费金额,币种,付费类型,等级,国家,地区,IP";
			writer.write("");
			writer.append(header + "\n");
			System.out.println(header);
			int count = 0;
			while ((line = reader.readLine()) != null) {
				PaymentLog log = new PaymentLog(line.split("\t"));
				String record = merge(
						new Object[] { log.getAccountID(), log.getAppID(), log.getPlatform(), log.getChannel(),
								log.getIdfa(), getDateTime(log.getPayTime()), log.getOrderId(),
								log.getCurrencyAmount(), log.getCurrencyType(), log.getPayType(), log.getLevel(),
								log.getCountry(), log.getProvince(), log.getExtend_4() }, ",");
				System.out.println(record);
				writer.append(record + "\n");
				count++;
			}
			System.out.println("count:" + count);
			reader.close();
			writer.close();
		}
	}

	/**
	 * <pre>
	 * 获取激活IDFA
	 * 1.先从原始日志读取，激活的IDFA，使用Map<IDFA,Time>>格式存储，IDFA和激活时间
	 * 2.从CP提供的文件中读取，所有的，使用List<String(time,idfs)>格式存储，
	 * 3.对比上面的集合，标记CP的IDFA中的激活标记，输出cvs文件
	 * 
	 * @author sam.xie
	 * @throws IOException 
	 * @throws NumberFormatException
	 * @date 2015年4月27日 下午3:41:42
	 */
	public void getActiveIDFA() throws NumberFormatException, IOException {
		String[] fileNames = { "idfa-xianmian.csv", "idfa-yewei.csv", "idfa-shujia.csv" };
		for (String fileName : fileNames) {
			int activeCount = 0;
			BufferedReader reader = new BufferedReader(new FileReader(
					"D:\\datatask\\xinxianjian-20150427\\idfa.new.0417-0427"));
			BufferedWriter writer = new BufferedWriter(new FileWriter("D:\\datatask\\xinxianjian-20150427\\"
					+ "active-" + fileName));
			Map<String, String> activeIdfa = new HashMap<String, String>();
			String line = "";
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			while (null != (line = reader.readLine())) {
				String[] arr = line.split("\t");
				String channel = arr[0];
				String idfa = convertJson2Map(arr[1]).get("IDFA");
				long ts = Integer.parseInt(arr[2]);
				String time = sdf.format(new Date(ts * 1000));
				activeIdfa.put(idfa, time);
			}
			System.out.println("激活IDFA总数：" + activeIdfa.size());
			reader.close();
			reader = new BufferedReader(new FileReader("D:\\datatask\\xinxianjian-20150427\\" + fileName));
			while (null != (line = reader.readLine())) {
				String[] arr = line.split(",");
				if (arr.length < 2) {
					continue;
				}
				String idfa = arr[1];
				String newLine = line;
				if (activeIdfa.containsKey(idfa)) {
					newLine = line + "," + "ACTIVE" + "," + activeIdfa.get(idfa);
					activeCount++;
				}
				newLine += "\n";
				writer.append(newLine);
			}
			reader.close();
			writer.close();
			System.out.println(fileName + ":" + activeCount);

		}
	}

	public void getDeviceInfoByAccountID() throws IOException {
		String path = "D:\\datatask\\huanlechuangtianxia\\";
		String file = "wh_online_day.0701";
		BufferedReader reader = new BufferedReader(new FileReader(path + file));
		Map<String, String> resultMap = new HashMap<String, String>();
		String line = "";
		while ((line = reader.readLine()) != null) {
			WHOnlineLog log = new WHOnlineLog(line.split("\t"));
			resultMap.put(log.getAccountId(), log.getAccountId() + "," + log.getBrand() + "," + log.getResolution()
					+ "," + log.getImei() + "," + log.getMac() + "," + log.getCountry() + "," + log.getProvince());
		}
		file = "uid_acc";
		reader.close();
		reader = new BufferedReader(new FileReader(path + file));
		BufferedWriter writer = new BufferedWriter(new FileWriter(path + file + ".账户设备.csv"));
		writer.write("帐号,机型,操作系统,IMEI,MAC,国家,省份\n");
		int counter = 0;
		while ((line = reader.readLine()) != null) {
			String detail = resultMap.get(line);
			if (detail == null) {
				detail = line + ",-,-,-,-";
			}
			System.out.println(detail);
			writer.append(detail + "\n");
			counter++;
		}
		reader.close();
		writer.close();
		System.out.println("-----" + counter);
	}

	public void getCleanData() throws IOException {
		String[] suffix = { "0605", "0606", "0607", "0608", "0609", "0610", "0611" };
		String path = "D:\\hdfs\\payment.clean.";
		Map<String, Float> uidAmount = new HashMap<String, Float>();
		Map<String, Integer> uidTimes = new HashMap<String, Integer>();
		Map<String, Float> appAmount = new HashMap<String, Float>();
		Map<String, Integer> appTimes = new HashMap<String, Integer>();
		Set<String> filterResult = new HashSet<String>();
		Integer total = 0;
		for (String str : suffix) {
			String file = path + str;
			String date = "2015" + str;
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = "";
			while ((line = br.readLine()) != null) {
				total++;
				String[] items = line.split("\t");
				String uid = items[0];
				String app = items[1];
				String rule = items[2];
				float amount = StringUtil.convertFloat(items[3], 0);
				int times = StringUtil.convertInt(items[4], 0);
				String currency = items[5];
				if (uid.startsWith("DEUID_")) {
					rule = "2.1";
				}
				if (rule.equals("-")) {
					if (null == uidAmount.get(uid)) {
						uidAmount.put(uid, amount);
					} else {
						uidAmount.put(uid, amount + uidAmount.get(uid));
					}
					if (null == uidTimes.get(uid)) {
						uidTimes.put(uid, times);
					} else {
						uidTimes.put(uid, times + uidTimes.get(uid));
					}
					if (null == appAmount.get(app)) {
						appAmount.put(app, amount);
					} else {
						appAmount.put(app, amount + appAmount.get(app));
					}
					if (null == appTimes.get(app)) {
						appTimes.put(app, times);
					} else {
						appTimes.put(app, times + appTimes.get(app));
					}
				} else {
					filterResult.add(merge(new String[] { uid, app, date, rule, amount + "", times + "", currency },
							","));
				}
			}
			br.close();
		}

		System.out.println("total:" + total);
		System.out.println("filter:" + filterResult.size());
		System.out.println("uid:" + uidAmount.size());
		System.out.println("app:" + appAmount.size());

		// 写文件
		String file1 = path + "filter";
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(file1)));
		bw.write("");
		for (String line : filterResult) {
			bw.append(line + "\n");
		}
		bw.close();

		String file2 = path + "app";
		bw = new BufferedWriter(new FileWriter(new File(file2)));
		bw.write("");
		for (String key : appAmount.keySet()) {
			String line = key + "," + appAmount.get(key) + "," + appTimes.get(key);
			System.out.println(line);
			bw.append(line + "\n");
		}
		bw.close();

		String file3 = path + "uid";
		bw = new BufferedWriter(new FileWriter(new File(file3)));
		bw.write("");
		for (String key : uidAmount.keySet()) {
			String line = key + "," + uidAmount.get(key) + "," + uidTimes.get(key);
			System.out.println(line);
			bw.append(line + "\n");
		}
		bw.close();

		System.out.println("total:" + total);
		System.out.println("filter:" + filterResult.size());
		System.out.println("uid:" + uidAmount.size());
		System.out.println("app:" + appAmount.size());

	}

	public static String merge(Object[] arr, String separator) {
		StringBuffer sb = new StringBuffer("");
		for (Object obj : arr) {
			sb.append(obj + ",");
		}
		if (sb.length() > 1) {
			return sb.substring(0, sb.length() - 1);
		}
		return sb.toString();
	}

	/**
	 * <pre>
	 * 将Json格式字符串转换为Map对象
	 * 这里只解析如{"age":123,"name":"sam"}这类简单的Json串
	 * 暂不支持JSON为列表，对象的情况
	 * @author sam.xie
	 * @date 2015年3月2日 下午8:26:05
	 */
	public static Map<String, String> convertJson2Map(String json) {
		Map<String, String> map = new LinkedHashMap<String, String>();
		if (StringUtils.isBlank(json)) {
			System.out.println("json is null" + json);
			return map;
		}
		try {
			JsonObject jsonObj = (JsonObject) jsonParser.parse(json);
			for (Entry<String, JsonElement> entry : jsonObj.entrySet()) {
				String key = entry.getKey();
				JsonElement element = entry.getValue();
				if (element.isJsonPrimitive()) {
					map.put(key, element.getAsString());
				}
			}
		} catch (Exception e) {
			logger.error("Json Syntax Error");

		}
		return map;
	}

	public static String getDateTime(long unixTimestamp) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(unixTimestamp * 1000);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(cal.getTime());
	}

	public static String getDate(long unixTimestamp) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(unixTimestamp * 1000);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		return sdf.format(cal.getTime());
	}

	public static void getDate() {
		Calendar cal = Calendar.getInstance();
		System.out.println(cal.getTimeInMillis());
		System.out.println(cal.getTime());
		cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 1);
		cal.set(Calendar.HOUR, cal.get(Calendar.HOUR) - 14);

		logger.info(cal.getTime().toString());
	}

	public void getHourStat() throws NumberFormatException, IOException {
		// Map<String, Integer> hourTimes = new HashMap<String, Integer>();
		// Map<String, Integer> hourDuration = new HashMap<String, Integer>();
		Map<String, Set<String>> hourUID = new HashMap<String, Set<String>>();
		String path = "D:\\hdfs\\";
		String fileName = path + "wh_online_day.201508";
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		BufferedWriter writer = new BufferedWriter(new FileWriter(fileName + ".csv"));
		String line = "";
		writer.write("");
		int count = 0;
		while ((line = reader.readLine()) != null) {
			String[] items = line.split("\t");
			String day = getDate(Integer.parseInt(items[0]));
			String uid = items[1];
			String record = items[2];
			for (String detail : record.split(",")) {
				String hour = detail.split(":")[0];
				// String times = detail.split(":")[1];
				// String duration = detail.split(":")[2];
				String key = day + hour;
				if (null == hourUID.get(key)) {
					Set<String> uidSet = new HashSet<String>();
					hourUID.put(key, uidSet);
				}
				hourUID.get(key).add(uid);
			}
			count++;
		}
		for (Entry<String, Set<String>> entry : hourUID.entrySet()) {
			String dayHour = entry.getKey();
			String day = dayHour.substring(0, 8);
			String hour = dayHour.substring(8, 10);			
			Integer num = entry.getValue().size();
			System.out.println(day + ":" + hour + ":" + num);
			
		} 
		System.out.println("count:" + count);
		reader.close();
		writer.close();
	}

	public static void printSet(Set<String> set) {
		System.out.println("------------------------------------------------------------" + set.size());
		for (String item : set) {
			System.out.println(item);
		}
	}

}
