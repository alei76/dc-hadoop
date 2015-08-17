package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 应用使用统计
 * 应用即资源，为了避免与渠道本身的App混淆，这里用Res表示
 * 
 * 
 * -----------------------------输入-----------------------------
 * 1.应用启动时间，持续时长
 * Key:				appID,country,province,packageName
 * Value:			resName,resVersion,launchTime,duration,uid
 * Value.Suffix:	Constants.SUFFIX_APP_LAUNCH
 * 
 * 2.应用安装
 * Key:				appID,country,province,packageName
 * Value:			resName,resVersion,installTime,uid
 * Value.Suffix:	Constants.SUFFIX_APP_INSTALL
 * 
 * 3.应用卸载
 * Key:				appID,country,province,packageName
 * Value:			resName,resVersion,uninstallTime,uid
 * Value.Suffix:	Constants.SUFFIX_APP_UNINSTALL
 * 
 * -----------------------------输出-----------------------------
 * 1.应用使用：启动次数汇总，在线时长汇总
 * Key:				appID,country,province,packageName
 * Key.Suffix:		Constants.SUFFIX_APP_USE_HABIT
 * Value:			resName,resVersion,activeNum,launchTimes,duration,installTimes,uninstallTimes
 * 最后一次登录/安装的应用名，最后一次登录/安装的版本号，活跃用户数，启动次数，每次启动的最大在线时长汇总，安装次数（按设备去重），卸载次数（按设备去重）
 * 
 * 
 * @author sam.xie
 * @date 2015年3月4日 上午10:56:50
 * @version 1.0
 */
public class CHAppUseHabitReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		/** 取最后一次安装，卸载，启动的应用名，版本号 */
		String lastResName = "-";
		String lastResVer = "-";
		String lastTime = "0";

		TreeMap<String, Integer> map = new TreeMap<String, Integer>(); // 保存同一次登录的最大在线时长
		Set<String> activeSet = new HashSet<String>(); // 活跃uid集合，使用Set去重
		Set<String> installSet = new HashSet<String>(); // 安装uid集合，使用Set去重
		Set<String> uninstallSet = new HashSet<String>(); // 卸载uid集合，使用Set去重

		for (OutFieldsBaseModel value : values) {
			if (Constants.SUFFIX_APP_LAUNCH.equals(value.getSuffix())) {
				// 取登录时间和最大在线时长 ，时间的自然排序
				String[] outFields = value.getOutFields();
				String resName = outFields[0];
				String resVer = outFields[1];
				String launchTime = outFields[2];
				int duration = StringUtil.convertInt(outFields[3], 0);
				String uid = outFields[4];
				if (null == map.get(launchTime)) {
					map.put(launchTime, duration);
				} else if (map.get(launchTime) < duration) { // 取最大时长
					map.put(launchTime, duration);
				}

				if (launchTime.compareTo(lastTime) > 0) { // 取最后一次登录的版本和账户名
					lastTime = launchTime;
					if (!"-".equals(resName)) {
						lastResName = resName;
					}
					if (!"-".equals(resVer)) {
						lastResVer = resVer;
					}
				}
				if ("-".equals(lastResName) && !"-".equals(resName)) {
					lastResName = resName;
				}
				if ("-".equals(lastResVer) && !"-".equals(resVer)) {
					lastResVer = resVer;
				}

				activeSet.add(uid);
			} else if (Constants.SUFFIX_APP_INSTALL.equals(value.getSuffix())) {
				// 取最后一次安装的应用名和版本号
				String[] outFields = value.getOutFields();
				String resName = outFields[0];
				String resVer = outFields[1];
				String installTime = outFields[2];
				String uid = outFields[3];

				if (installTime.compareTo(lastTime) > 0) { // 取最后一次安装的版本和账户名
					lastTime = installTime;
					if (!"-".equals(resName)) {
						lastResName = resName;
					}
					if (!"-".equals(resVer)) {
						lastResVer = resVer;
					}
				}
				if ("-".equals(lastResName) && !"-".equals(resName)) {
					lastResName = resName;
				}
				if ("-".equals(lastResVer) && !"-".equals(resVer)) {
					lastResVer = resVer;
				}
				installSet.add(uid);
			} else if (Constants.SUFFIX_APP_UNINSTALL.equals(value.getSuffix())) {
				// 取最后一次卸载的应用名和版本号
				String[] outFields = value.getOutFields();
				String resName = outFields[0];
				String resVer = outFields[1];
				String uninstallTime = outFields[2];
				String uid = outFields[3];

				if (uninstallTime.compareTo(lastTime) > 0) { // 取最后一次卸载的版本和账户名
					lastTime = uninstallTime;
					if (!"-".equals(resName)) {
						lastResName = resName;
					}
					if (!"-".equals(resVer)) {
						lastResVer = resVer;
					}
				}
				if ("-".equals(lastResName) && !"-".equals(resName)) {
					lastResName = resName;
				}
				if ("-".equals(lastResVer) && !"-".equals(resVer)) {
					lastResVer = resVer;
				}
				uninstallSet.add(uid); // 设备号去重
			}
		}

		// 累计登录次数，总在线时长
		int totalLaunchTimes = map.size();
		int totalDuration = 0;
		for (Integer value : map.values()) {
			totalDuration += value;
		}
		int totalActiveNum = activeSet.size();
		int totalInstallTimes = installSet.size();
		int totalUninstallTimes = uninstallSet.size();
		OutFieldsBaseModel outputValue = new OutFieldsBaseModel();
		String[] valueFileds = { lastResName, lastResVer, totalActiveNum + "", totalLaunchTimes + "",
				totalDuration + "", totalInstallTimes + "", totalUninstallTimes + "" };
		outputValue.setOutFields(valueFileds);
		key.setSuffix(Constants.SUFFIX_APP_USE_HABIT);
		context.write(key, outputValue);
	}

}
