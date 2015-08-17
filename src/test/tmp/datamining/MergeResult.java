package test.tmp.datamining;

import java.io.BufferedReader;
import java.io.FileReader;
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
public class MergeResult {

	public List<String> readFile(String file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		List<String> list = new ArrayList<String>();
		String line = "";
		while (null != (line = reader.readLine())) {
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
				String item2 = o2.split("\t")[3];
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
							return 1;
						}
					}
					// return 0;
				}
			}
		});
		return list;
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

	// @Test
	public void getGrade() throws IOException {
		String file = "D:\\tmpdata\\helloketty3\\grade.051617";
		List<String> list = readFile(file);
		for (String line : list) {
			System.out.println(line.replace("\t", ","));
		}
	}

	// @Test
	public void getItem() throws IOException {
		String file1 = "D:\\tmpdata\\helloketty3\\grade.051617";
		String file = "D:\\tmpdata\\helloketty3\\item.051617";
		List<String> list = readFile(file);
		Map<String, Integer> succMap = getPlayerCount(file1, 3);
		Map<String, Integer> allMap = getPlayerCount(file1, 6);
		for (String line : list) {
			String[] attr = line.split("\t");
			String appID = attr[0];
			String version = attr[1];
			String grade = attr[2];
			String item = attr[3].replace("ALL_ITEM", "[所有道具]");
			int num = StringUtil.convertInt(attr[4], 0);
			String key = appID + "," + version + "," + grade;
			int succNum = succMap.get(key) == null ? 0 : succMap.get(key);
			int allNum = allMap.get(key) == null ? 0 : allMap.get(key);
			String succPercent = succNum <= 0 ? "-" : StringUtil.getFloatString(((float) num / (float) succNum) * 100,
					2) + "%";
			String allPercent = allNum <= 0 ? "-" : StringUtil.getFloatString(((float) num / (float) allNum) * 100, 2)
					+ "%";
			System.out.println(appID + "," + version + "," + grade + "," + item + "," + num + "," + succNum + ","
					+ allNum + "," + succPercent + "," + allPercent);
		}
	}

	@Test
	public void getStar() throws IOException {
		String file1 = "D:\\tmpdata\\helloketty3\\grade.05161718";
		String file = "D:\\tmpdata\\helloketty3\\star.05161718";
		List<String> list = readFile(file);
		// key = appID + version + grade
		Map<String, Integer> succMap = getPlayerCount(file1, 3);
		Map<String, Integer> allStarMap = new HashMap<String, Integer>();
		LinkedHashMap<String, Map<String, Integer>> resultMap = new LinkedHashMap<String, Map<String, Integer>>();
		TreeSet<String> starSet = new TreeSet<String>();
		for (String line : list) {
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
		sb.append("APPID," + "版本," + "关卡," +"独立通关人数");
		for (String star : starSet) {
			sb.append("," + star);
			
		}
		System.out.println(sb);
		for (Entry<String, Map<String, Integer>> entry : resultMap.entrySet()) {
			sb.delete(0, sb.length());
			String appidVersionGrade = entry.getKey();
			Map<String, Integer> starMap = entry.getValue();
//			Integer totalNum = succMap.get(appidVersionGrade);
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
		}
	}
}
