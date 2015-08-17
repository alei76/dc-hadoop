package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre/>
 * 
 * 设备应用列表
 * @see WHUidAppListRollingMapper
 * 
 * @author sam.xie
 * @date 2015年8月6日 下午4:25:30
 * @version 1.0
 */
public class WHUidAppListRollingReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private Set<String> currentAppSet = new HashSet<String>();
	private Set<String> lastAppSet = new HashSet<String>();
	private Set<String> pastAppSet = new HashSet<String>();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		currentAppSet.clear();
		lastAppSet.clear();
		pastAppSet.clear();
		int currentUpdateTime = 0;
		int lastUpdateTime = 0;
		for (OutFieldsBaseModel val : values) {
			if (val.getSuffix().equals("D")) {
				int tmpTime = StringUtil.convertInt(val.getOutFields()[0], 0);
				String appName = val.getOutFields()[1];
				if (currentUpdateTime < tmpTime) { // 取最后更新时间
					currentUpdateTime = tmpTime;
				}
				if ((!StringUtil.isEmpty(appName))) {
					currentAppSet.add(appName);
				}
			} else if (val.getSuffix().equals("R")) {
				int tmpTime = StringUtil.convertInt(val.getOutFields()[0], 0);
				String lastAppList = val.getOutFields()[1];
				String pastAppList = val.getOutFields()[2];
				if (lastUpdateTime < tmpTime) { // 取最后更新时间
					lastUpdateTime = tmpTime;
				}

				if ((!StringUtil.isEmpty(lastAppList)) && (!"-".equals(lastAppList))) {
					String[] appNames = lastAppList.split(",");
					for (String appName : appNames) {
						lastAppSet.add(appName);
					}
				}

				if ((!StringUtil.isEmpty(pastAppList)) && (!"-".equals(pastAppList))) {
					String[] appNames = lastAppList.split(",");
					for (String appName : appNames) {
						pastAppSet.add(appName);
					}
				}

			}
		}

		if (currentUpdateTime > lastUpdateTime) {
			// 有上报最新应用列表，就将最新应用作为lastAppSet
			// 而原来的lastAppSet与pastAppSet合并，同时排除currentAppset重合的部分
			pastAppSet.addAll(lastAppSet);
			pastAppSet.removeAll(currentAppSet);
			valFields.setOutFields(new String[] { currentUpdateTime + "", setToString(currentAppSet),
					setToString(pastAppSet) });
		} else {
			valFields.setOutFields(new String[] { lastUpdateTime + "", setToString(lastAppSet),
					setToString(pastAppSet) });
		}
		try {
			context.write(key, valFields);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("key:" + Arrays.toString(key.getOutFields()) + ",value:"
					+ Arrays.toString(valFields.getOutFields()));
		}

	}

	private String setToString(Set<String> set) {
		if (set.size() > 0) {
			return StringUtil.join(set, ",");
		}
		return "-";
	}
}
