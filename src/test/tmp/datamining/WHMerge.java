package test.tmp.datamining;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.digitcube.hadoop.mapreduce.warehouse.common.OnlineAndPay;
import net.digitcube.hadoop.util.StringUtil;

public class WHMerge {
	List<String> locationList = new ArrayList<String>();
	List<String> hourList = new ArrayList<String>();
	Map<String, String[]> uidLocation = new HashMap<String, String[]>();
	Map<String, OnlineAndPay> totalMap = new HashMap<String, OnlineAndPay>();
	Map<String, Set<String>> uidMacMap = new HashMap<String, Set<String>>();
	Map<String, String> uidChannelMap = new HashMap<String, String>();

	public void resolveMac() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader("D:\\hdfs\\warehouse\\tmp_mac_uid.log"));
		String line = "";
		while (null != (line = reader.readLine())) {
			String[] arr = line.split("\t");
			String channel = arr[0];
			String uid = arr[1];
			String mac = arr[2];
			Set<String> macSet = uidMacMap.get(uid);
			if (macSet == null) {
				macSet = new HashSet<String>();
				uidMacMap.put(uid, macSet);
			}
			macSet.add(mac);
			uidChannelMap.put(uid, channel);
		}
		reader.close();
	}

	public void resolveLocation() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader("D:\\hdfs\\warehouse\\tmp_location.log"));
		String line = "";
		while (null != (line = reader.readLine())) {
			String[] arr = line.split("\t");
			String uid = arr[0];
			String country = arr[1];
			String province = arr[2];
			uidLocation.put(uid, new String[] { country, province });
			System.out.println("uidLocation>" + uid + ":" + country + ":" + province);
		}
		reader.close();
	}

	public void resolveHour() throws IOException {
		resolveLocation();
		resolveMac();
		BufferedReader reader = new BufferedReader(new FileReader("D:\\hdfs\\warehouse\\tmp_hour.log"));
		String line = "";
		while (null != (line = reader.readLine())) {
			String[] arr = line.split("\t");
			String uid = arr[0];
			String appid = arr[1];
			int dim = StringUtil.convertInt(arr[3], 0);
			int value = StringUtil.convertInt(arr[4], 0);
			String keyTotal = uid + "\t" + appid;
			OnlineAndPay total = totalMap.get(keyTotal);

			// 累计统计
			if (null == total) {
				String[] location = uidLocation.get(uid);
				if (null == location) {
					location = new String[] { "-", "-" };
				}
				total = new OnlineAndPay(new String[] { uidMacMap.get(uid).size() + "", uidChannelMap.get(uid), uid,
						appid, location[0], location[1] });
				totalMap.put(keyTotal, total);
			}
			if (dim == 1) {
				total.setLoginTimes(value);
			} else if (dim == 2) {
				total.setDuration(value);
			} else if (dim == 3) {
				total.setPayTimes(value);
			} else if (dim == 4) {
				total.setPayAmount(value);
			}
			System.out.println("totalMap>" + total.toSimpleString());
		}
		reader.close();
	}

	private void writeFile() throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter("D:\\hdfs\\warehouse\\result_total.csv"));
		for (OnlineAndPay item : totalMap.values()) {
			System.out.println("writeFile>" + item.toSimpleString());
			String line = item.getKeys()[0] + "," + item.getKeys()[1] + "," + item.getKeys()[2] + ","
					+ item.getKeys()[3] + "," + item.getKeys()[4] + "," + item.getKeys()[5] + ","
					+ item.getLoginTimes() + "," + item.getDuration() + "," + item.getPayTimes() + ","
					+ item.getPayAmount();
			bw.write(line + "\n");
		}
		bw.close();
	}

	public static void main(String[] args) throws IOException {
		WHMerge test = new WHMerge();
		test.resolveHour();
		test.writeFile();
	}

}
