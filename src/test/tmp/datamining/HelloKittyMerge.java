package test.tmp.datamining;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import net.digitcube.hadoop.util.StringUtil;

import org.junit.Test;

/**
 * 将日志合并成报表
 * 
 * @author sam.xie
 * @date 2015年5月19日 下午7:10:33
 * @version 1.0
 */
public class HelloKittyMerge {

	private static final String PATH = "D:\\datatask\\hellokitty\\";
	private static final String GRADE = "grade.";
	private static final String ITEM = "item.";
	private static final String STAR = "star.";
	private static final String[] dates = { "0603-0608" };// { "0603", "0604", "0605", "0606", "0607", "0608" };

	public List<String> readFile(String file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		List<String> list = new ArrayList<String>();
		String line = "";
		while (null != (line = reader.readLine())) {
			String[] arr = line.split("\t");
			String version = arr[1];
			// 过滤版本
			// if ("1.1".equals(version)) {
			// }
			list.add(line);
		}

		reader.close();
		Collections.sort(list, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				String version1 = o1.split("\t")[1];
				String version2 = o2.split("\t")[1];
				int grade1 = StringUtil.convertInt(o1.split("\t")[2].substring("关卡".length()), 9999);
				int grade2 = StringUtil.convertInt(o2.split("\t")[2].substring("关卡".length()), 9999);
				String item1 = o1.split("\t")[3];
				// String item2 = o2.split("\t")[3];
				if (o1.equals(o2)) {
					System.out.println(o1);
				}
				if (version1.compareTo(version2) > 0) {
					return 1;
				} else if (version1.compareTo(version2) < 0) {
					return -1;
				} else {
					if (grade1 < grade2) {
						return -1;
					} else if (grade1 > grade2) {
						return 1;
					} else {
						if (item1.contains("ALL_")) {
							return -1;
						} else {
							return 0;
						}
					}
					// return 0;
				}
			}
		});
		return list;
	}

	public void writeFile(String file, List<String> list) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(file + ".csv"));// 输出为csv文件
		bw.write("");
		for (String line : list) {
			bw.append(line + "\n");
		}
		bw.close();
	}

	/**
	 * index = 3, 通关玩家 index = 6, 所有玩家
	 */
	public Map<String, Integer> getPlayerCount(String file, int index) throws IOException {
		List<String> list = readFile(file);
		Map<String, Integer> map = new HashMap<String, Integer>();
		for (String line : list) {
			String[] items = line.split("\t");
			String key = items[0] + "," + items[1] + "," + items[2];
			int count = StringUtil.convertInt(items[index], 0);
			map.put(key, count);
		}
		return map;
	}

	 @Test
	public void getGrade() throws IOException {
		for (String date : dates) {
			String gradeFile = PATH + GRADE + date;
			List<String> readList = readFile(gradeFile);
			List<String> writeList = new ArrayList<String>();
			writeList.add("APPID,版本,关卡,首次通关人数,成功前尝试次数,第一次成功通关总耗时,所有玩家");
			for (String line : readList) {
				writeList.add(line.replace("\t", ","));
			}
			writeFile(PATH + GRADE + date, writeList);
		}
	}

	 @Test
	public void getItem() throws IOException {
		for (String date : dates) {
			String itemFile = PATH + ITEM + date;
			String gradeFile = PATH + GRADE + date;
			List<String> readList = readFile(itemFile); // 获取道具
			Map<String, Integer> succMap = getPlayerCount(gradeFile, 3); // 从关卡总获取成功次数
			Map<String, Integer> allMap = getPlayerCount(gradeFile, 6); // 从关卡总获取总次数
			List<String> writeList = new ArrayList<String>();
			writeList.add("APPID,版本,关卡,道具,道具玩家,通关玩家,所有玩家,道具玩家/通关玩家,道具玩家/所有玩家");
			for (String line : readList) {
				String[] attr = line.split("\t");
				String appID = attr[0];
				String version = attr[1];
				String grade = attr[2];
				String item = attr[3].replace("ALL_ITEM", "[所有道具]");
				int num = StringUtil.convertInt(attr[4], 0);
				String key = appID + "," + version + "," + grade;
				int succNum = succMap.get(key) == null ? 0 : succMap.get(key);
				int allNum = allMap.get(key) == null ? 0 : allMap.get(key);
				String succPercent = succNum <= 0 ? "-" : StringUtil.getFloatString(
						((float) num / (float) succNum) * 100, 2) + "%";
				String allPercent = allNum <= 0 ? "-" : StringUtil.getFloatString(((float) num / (float) allNum) * 100,
						2) + "%";
				writeList.add(appID + "," + version + "," + grade + "," + item + "," + num + "," + succNum + ","
						+ allNum + "," + succPercent + "," + allPercent);
			}
			writeFile(itemFile, writeList);
		}
	}

	@Test
	public void getStar() throws IOException {
		for (String date : dates) {
			String gradeFile = PATH + GRADE + date;
			String starFile = PATH + STAR + date;
			List<String> readList = readFile(starFile);
			// key = appID + version + grade
			Map<String, Integer> succMap = getPlayerCount(gradeFile, 3);
			Map<String, Integer> allStarMap = new HashMap<String, Integer>();
			List<String> writeList = new ArrayList<String>();
			LinkedHashMap<String, Map<String, Integer>> resultMap = new LinkedHashMap<String, Map<String, Integer>>();
			TreeSet<String> starSet = new TreeSet<String>();
			for (String line : readList) {
				String[] attr = line.split("\t");
				String appID = attr[0];
				String version = attr[1];
				String grade = attr[2];
				String star = attr[3];
				String key = appID + "," + version + "," + grade;
				int num = StringUtil.convertInt(attr[4], 0);
				if (star.startsWith("ALL")) {
					allStarMap.put(key, num);
					continue;
				}
				Map<String, Integer> starMap = resultMap.get(key);
				if (null == starMap) {
					starMap = new HashMap<String, Integer>();
				}
				starMap.put(star, num);
				resultMap.put(key, starMap);
				starSet.add(star);
			}
			StringBuilder sb = new StringBuilder("");
			sb.append("APPID,版本,关卡,独立通关人数");
			for (String star : starSet) {
				sb.append("," + star);

			}
			System.out.println(sb);
			writeList.add(sb.toString());
			for (Entry<String, Map<String, Integer>> entry : resultMap.entrySet()) {
				sb.delete(0, sb.length());
				String appidVersionGrade = entry.getKey();
				Map<String, Integer> starMap = entry.getValue();
				// Integer totalNum = succMap.get(appidVersionGrade);
				Integer totalNum = allStarMap.get(appidVersionGrade);
				sb.append(appidVersionGrade).append("," + totalNum);
				for (String star : starSet) {
					Integer num = starMap.get(star);
					if (null == num) {
						sb.append(",-");
					} else {
						if (totalNum <= 0) {
							sb.append(",-");
						} else {
							sb.append("," + StringUtil.getFloatString((float) num / (float) totalNum * 100, 2) + "%");
						}
					}
				}
				System.out.println(sb);
				writeList.add(sb.toString());
			}
			writeFile(starFile, writeList);
		}
	}
}
