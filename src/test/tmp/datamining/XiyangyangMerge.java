package test.tmp.datamining;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

/**
 * 将日志合并成报表
 * 
 * @author sam.xie
 * @date 2015年7月08日 下午20:57:33
 * @version 1.0
 */
public class XiyangyangMerge {

	public Calendar cal = Calendar.getInstance();
	public SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public static void main(String[] args) throws IOException {
		XiyangyangMerge merge = new XiyangyangMerge();
		 merge.resolve_Coin_Gain();
		 merge.resolve_Coin_Lost();
		// merge.resolve_ItemGet();
		// merge.resolve_ItemBuy();
		// merge.resolve_ItemUse();
//		merge.resolve_LevelsBegin();
	}

	public void resolve_Coin_Gain() throws IOException {
		String file = "D:\\datatask\\shaonanmoshoutuan\\part-r-00000-DESelf_Coin_Gain";
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = "";
		List<String> resultList = new ArrayList<String>();
		resultList.add(mergeArr(new String[] { "日期", "AccountID", "渠道", "虚拟币类型", "获得原因", "数量" }, ","));
		while (null != (line = reader.readLine())) {
			String[] arr = line.split("\t");
			EventLog log = new EventLog(arr);
			long ts = Long.parseLong(log.getTimestamp());
			cal.setTimeInMillis(ts);
			String date = sdf.format(cal.getTime());
			String accountId = log.getAccountID();
			String channel = log.getChannel();
			// String appId = log.getPlatform();
			// String platform = log.getPlatform();
			// String mac = log.getMac();
			// mac = StringUtil.isEmpty(mac) ? "-" : mac;
			// String imei = log.getImei();
			// imei = StringUtil.isEmpty(imei) ? "-" : imei;
			// String idfa = log.getIdfa();
			// idfa = StringUtil.isEmpty(idfa) ? "-" : idfa;
			// String gameServer = log.getGameServer();

			String coinType = log.getArrtMap().get("coinType");
			coinType = null == coinType ? Constants.DEFAULT_COIN_TYPE : coinType;

			String reason = log.getArrtMap().get("id");
			reason = StringUtil.isEmpty(reason) ? "-" : reason;
			String num = log.getArrtMap().get("num");
			String result = mergeArr(new String[] {date, accountId, channel, coinType, reason, num }, ",");
			resultList.add(result);
			System.out.println(result);
		}
		writeFile(file, resultList);
		reader.close();
	}

	public void resolve_Coin_Lost() throws IOException {
		String file = "D:\\datatask\\shaonanmoshoutuan\\part-r-00000-DESelf_Coin_Lost";
		BufferedReader reader = new BufferedReader(new FileReader(file));
		List<String> resultList = new ArrayList<String>();
		resultList.add(mergeArr(new String[] { "日期", "AccountID", "渠道", "虚拟币类型", "消费原因", "数量" }, ","));
		String line = "";
		while (null != (line = reader.readLine())) {
			String[] arr = line.split("\t");
			EventLog log = new EventLog(arr);
			long ts = Long.parseLong(log.getTimestamp());
			cal.setTimeInMillis(ts);
			String date = sdf.format(cal.getTime());
			String accountId = log.getAccountID();
			String channel = log.getChannel();

			String coinType = log.getArrtMap().get("coinType");
			coinType = null == coinType ? Constants.DEFAULT_COIN_TYPE : coinType;

			String reason = log.getArrtMap().get("id");
			reason = StringUtil.isEmpty(reason) ? "-" : reason;
			String num = log.getArrtMap().get("num");
			String result = mergeArr(new String[] {date, accountId, channel, coinType, reason, num }, ",");
			resultList.add(result);
			System.out.println(result);
		}
		writeFile(file, resultList);
		reader.close();
	}

	public void resolve_ItemGet() throws IOException {
		String file = "D:\\datatask\\xiyangyangpaopaodazhan\\part-r-00000-DESelf_ItemGet";
		BufferedReader reader = new BufferedReader(new FileReader(file));
		List<String> resultList = new ArrayList<String>();
		resultList.add(mergeArr(new String[] { "AccountID", "渠道", "道具ID", "道具类型", "获取原因", "数量" }, ","));
		String line = "";
		while (null != (line = reader.readLine())) {
			String[] arr = line.split("\t");
			EventLog log = new EventLog(arr);
			String accountId = log.getAccountID();
			String channel = log.getChannel();

			String itemId = log.getArrtMap().get("itemId");
			String itemType = log.getArrtMap().get("itemType");
			String itemCnt = log.getArrtMap().get("itemCnt");
			String reason = log.getArrtMap().get("reason");

			String result = mergeArr(new String[] { accountId, channel, itemId, itemType, reason, itemCnt }, ",");
			resultList.add(result);
			System.out.println(result);
		}
		writeFile(file, resultList);
		reader.close();
	}

	public void resolve_ItemBuy() throws IOException {
		String file = "D:\\datatask\\xiyangyangpaopaodazhan\\part-r-00000-DESelf_ItemBuy";
		BufferedReader reader = new BufferedReader(new FileReader(file));
		List<String> resultList = new ArrayList<String>();
		resultList.add(mergeArr(new String[] { "AccountID", "渠道", "道具ID", "道具类型", "消费点", "数量" }, ","));
		String line = "";
		while (null != (line = reader.readLine())) {
			String[] arr = line.split("\t");
			EventLog log = new EventLog(arr);
			String accountId = log.getAccountID();
			String channel = log.getChannel();

			String itemId = log.getArrtMap().get("itemId");
			String itemType = log.getArrtMap().get("itemType");
			String itemCnt = log.getArrtMap().get("itemCnt");
			String reason = log.getArrtMap().get("consumePoint");

			String result = mergeArr(new String[] { accountId, channel, itemId, itemType, reason, itemCnt }, ",");
			resultList.add(result);
			System.out.println(result);
		}
		writeFile(file, resultList);
		reader.close();
	}

	public void resolve_ItemUse() throws IOException {
		String file = "D:\\datatask\\xiyangyangpaopaodazhan\\part-r-00000-DESelf_ItemUse";
		BufferedReader reader = new BufferedReader(new FileReader(file));
		List<String> resultList = new ArrayList<String>();
		resultList.add(mergeArr(new String[] { "AccountID", "渠道", "道具ID", "道具类型", "使用原因", "数量" }, ","));
		String line = "";
		while (null != (line = reader.readLine())) {
			String[] arr = line.split("\t");
			EventLog log = new EventLog(arr);
			String accountId = log.getAccountID();
			String channel = log.getChannel();

			String itemId = log.getArrtMap().get("itemId");
			String itemType = log.getArrtMap().get("itemType");
			String itemCnt = log.getArrtMap().get("itemCnt");
			String reason = log.getArrtMap().get("reason");

			String result = mergeArr(new String[] { accountId, channel, itemId, itemType, reason, itemCnt }, ",");
			resultList.add(result);
			System.out.println(result);
		}
		writeFile(file, resultList);
		reader.close();
	}

	public void resolve_LevelsBegin() throws IOException {
		String file = "D:\\datatask\\xiyangyangpaopaodazhan\\part-r-00000-DESelf_LevelsBegin";
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Set<String> resultSet = new HashSet<String>();
		String line = "";
		Map<String, Integer> map = new HashMap<String, Integer>();
		while (null != (line = reader.readLine())) {
			String[] arr = line.split("\t");
			EventLog log = new EventLog(arr);
			String accountId = log.getAccountID();
			String channel = log.getChannel();

			int currentlevelId = Integer.parseInt(log.getArrtMap().get("levelId"));
			Integer levelId = map.get(accountId);
			if (null == levelId) {
				levelId = currentlevelId;
			} else {
				levelId = levelId > currentlevelId ? levelId : currentlevelId;
			}
			map.put(accountId, levelId);
		}
		writeFile(file, map);
		reader.close();
	}

	public void writeFile(String file, List<String> list) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(file + ".csv"));// 输出为csv文件
		bw.write("");
		for (String line : list) {
			bw.append(line + "\n");
		}
		bw.close();
	}

	public void writeFile(String file, Map<String, Integer> map) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(file + ".csv"));// 输出为csv文件
		bw.write("");
		for (Entry<String, Integer> entry : map.entrySet()) {
			bw.append(entry.getKey() + "," + entry.getValue() + "\n");
		}
		bw.close();
	}

	public String mergeArr(String[] arr, String seperator) {
		StringBuilder sb = new StringBuilder("");
		for (String item : arr) {
			sb.append(item + seperator);
		}
		return sb.toString();
	}
}
