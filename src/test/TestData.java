package test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
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
import java.util.logging.SimpleFormatter;

import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.model.UserInfoLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.xunlei.util.Log;

public class TestData {

	public static JsonParser jsonParser = new JsonParser();

	private static Logger logger = Log.getLogger("Test");

	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
		TestData test = new TestData();
		// test.getXiaoMieXingXing();
		// test.getNewUID();
		// test.getActiveIDFA();
		test.getActDevice();
		// test.getnew();
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
	 * 解析设备激活
	 * 
	 * @throws IOException
	 */
	public void getActDevice() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader("D:\\tmpdata\\xinxianjian\\0508\\online.0518"));
		Map<String, String> uidMap = new HashMap<String, String>();
		String line = "";
		while ((line = reader.readLine()) != null) {
			String[] arr = line.split("\t");

			uidMap.put(line.split("\t")[2],
					arr[2] + "," + arr[12] + "," + arr[13] + "," + arr[19].split(",")[0].split(":")[0]);
		}
//		System.out.println(uidMap.size());
		reader.close();

		reader = new BufferedReader(new FileReader("D:\\tmpdata\\xinxianjian\\0508\\userinfo.logserver.0518"));
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while ((line = reader.readLine()) != null) {
			sb.delete(0, sb.length());
			UserInfoLog log = new UserInfoLog(line.split("\t"));
			if (uidMap.keySet().contains(log.getAccountID())) {
				if (log.getRegTime() > 0) { // 注册时间 > 0
					uidMap.remove(log.getAccountID());
					sb.append(log.getAccountID() + "," + log.getCountry() + "," + log.getProvince() + ","
							+ log.getRegTime() + "," + log.getImei() + "," + log.getMac() + "," + log.getExtend_4());
					System.out.println(sb);
					index++;
				}
			}
		}
		// 还有一部分没有上报注册日志，但是确实是新增玩家
		for (Entry<String, String> entry : uidMap.entrySet()) {
			System.out.println(entry.getValue());
		}
		reader.close();
//		System.out.println("-----" + index);
//		System.out.println("-----" + uidMap.size());
	}

	public void getXiaoMieXingXing() throws NumberFormatException, IOException {
		Set<String> deIMEI = new HashSet<String>();
		Set<String> otherIMEI = new HashSet<String>();
		BufferedReader reader = new BufferedReader(new FileReader("D:\\tmpdata\\IMEI.log"));
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
		reader = new BufferedReader(new FileReader("D:\\tmpdata\\xingxing.log"));
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
		BufferedReader reader = new BufferedReader(new FileReader("D:\\tmpdata\\userinfo-regtime.20150421.log"));
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
		reader = new BufferedReader(new FileReader("D:\\tmpdata\\online-uid.20150421.log"));
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
					"D:\\tmpdata\\xinxianjian-20150427\\idfa.new.0417-0427"));
			BufferedWriter writer = new BufferedWriter(new FileWriter("D:\\tmpdata\\xinxianjian-20150427\\" + "active-"
					+ fileName));
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
			reader = new BufferedReader(new FileReader("D:\\tmpdata\\xinxianjian-20150427\\" + fileName));
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

	public static void getDate() {
		Calendar cal = Calendar.getInstance();
		System.out.println(cal.getTimeInMillis());
		System.out.println(cal.getTime());
		cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 1);
		cal.set(Calendar.HOUR, cal.get(Calendar.HOUR) - 14);

		logger.info(cal.getTime().toString());
	}

	public static void printSet(Set<String> set) {
		System.out.println("------------------------------------------------------------" + set.size());
		for (String item : set) {
			System.out.println(item);
		}
	}

}
