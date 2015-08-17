package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.AppDetail;
import net.digitcube.hadoop.mapreduce.domain.AppRollingLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * <pre>
 * 
 * key: appID version platform channel gameserver uid
 * R 			:AppRollingLog
 * APPLIST_APP  :packageName,appName,appVer,installTime,lastAccessTime,setupTimes,upTime,ext
 * APPLIST_RING :packageName,appName,appVer,installTime,lastAccessTime,setupTimes,upTime,ext
 * @author Ivan          <br>
 * @date 2014-6-21 下午1:16:26 <br>
 * @version 1.0
 * <br>
 */
public class PhoneAppReducer extends Reducer<BigFieldsBaseModel, BigFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel outputKey = new OutFieldsBaseModel();

	@Override
	protected void reduce(BigFieldsBaseModel key, Iterable<BigFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		AppRollingLog appRollingLog = null;
		List<AppDetail> appList = new ArrayList<AppDetail>();
		List<AppDetail> ringList = new ArrayList<AppDetail>();
		// 用于过滤
		Map<String, AppDetail> appMap = new HashMap<String, AppDetail>();
		String maxVersion = "";
		String currentChannel = "";
		Set<String> ringNameSet = new HashSet<String>();
		for (BigFieldsBaseModel value : values) {
			String[] valueArr = value.getOutFields();
			// 判断value的后缀
			String valueSuffix = value.getSuffix();
			if ("R".equals(valueSuffix)) {
				// 滚存
				appRollingLog = new AppRollingLog(valueArr);
			} else {
				String packageName = valueArr[0];
				String appName = valueArr[1];
				String appVer = valueArr[2];
				int installTime = StringUtil.convertInt(valueArr[3], 0);
				int lastAccessTime = StringUtil.convertInt(valueArr[4], 0);
				int setUpTimes = StringUtil.convertInt(valueArr[5], 0);
				int uptime = StringUtil.convertInt(valueArr[6], 0);
				String ext = valueArr[7];
				String appVersion = valueArr[8];
				currentChannel = valueArr[9];

				AppDetail appDetail = new AppDetail(packageName, appName, appVer, installTime, lastAccessTime,
						setUpTimes, uptime, ext);
				// 因为可能存在重复上报的情况，在这里需要进行过滤
				if (Constants.SUFFIX_APPLIST_APP.equals(valueSuffix)) {
					AppDetail appDetailTmp = appMap.get(appName);
					// 没有就加入
					if (appDetailTmp == null) {
						appMap.put(appName, appDetail);
						// 存入当前的版本号
						maxVersion = appVersion;
						appList.add(appDetail);
					} else {
						// 已经存在了就更新setupTimes upTime,取较大的
						appDetailTmp.setSetUpTimes(Math.max(setUpTimes, appDetailTmp.getSetUpTimes()));
						appDetailTmp.setUptime(Math.max(uptime, appDetailTmp.getUptime()));
						// 设置最大版本号
						if (appVersion.compareTo(maxVersion) == 1) {
							maxVersion = appVersion;
						}
					}
				} else if (Constants.SUFFIX_APPLIST_RINGTONE.equals(valueSuffix)) {
					if (!ringNameSet.contains(appName)) {
						ringNameSet.add(appName);
						ringList.add(appDetail);
						maxVersion = appVersion;
					} else {
						// 大 版本
						if (appVersion.compareTo(maxVersion) == 1) {
							maxVersion = appVersion;
						}
					}
				}
			}
		}
		// 滚存以及去重以后的应用+铃声信息都准备好了
		// 下面就开始比较了
		if (appRollingLog == null) {
			// 该设备没有滚存日志，是第一次上报
			newUid(key, appList, ringList, context, maxVersion, currentChannel);
		} else {
			appRollingLog.setChannel(currentChannel);
			if (appList.size() > 0 || ringList.size() > 0) {
				// 该设备已经上报过信息了，就需要进行比较得到这次比上次新安装了哪些应用或者卸载了哪些应用
				oldApp(key, appRollingLog, appList, context, maxVersion, currentChannel);
				oldRing(key, appRollingLog, ringList, context, maxVersion, currentChannel);
				// 把最后一次上报的写入滚存
				appRollingLog.setAppList(appList);
				appRollingLog.setRingList(ringList);

			}
			// 继续写入滚存
			String[] rollingKey = appRollingLog.toStringArray();
			outputKey.setOutFields(rollingKey);
			outputKey.setSuffix(Constants.SUFFIX_APPLIST_ROLLING_DAY);
			context.write(outputKey, NullWritable.get());
		}
	}

	/**
	 * 处理第一次上报的情况
	 * 
	 * @param key
	 * @param appList
	 * @param ringList
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void newUid(BigFieldsBaseModel key, List<AppDetail> appList, List<AppDetail> ringList, Context context,
			String maxVersion, String currentChannel) throws IOException, InterruptedException {
		// appID platform channel gameserver uid
		String[] keyArr = key.getOutFields();

		// 先统计应用和铃声的安装情况（因为没有历史滚存肯定都是新增）
		for (AppDetail appDetail : appList) {
			String[] kArr = new String[] { keyArr[0], maxVersion, keyArr[1], currentChannel, keyArr[2],
					appDetail.getAppName(), "1", appDetail.getSetUpTimes() + "", appDetail.getUptime() + "" };
			outputKey.setOutFields(kArr);
			outputKey.setSuffix(Constants.SUFFIX_APPLIST_APP);
			context.write(outputKey, NullWritable.get());
		}
		for (AppDetail appDetail : ringList) {
			String[] kArr = new String[] { keyArr[0], maxVersion, keyArr[1], currentChannel, keyArr[2],
					appDetail.getAppName(), "1" };
			outputKey.setOutFields(kArr);
			outputKey.setSuffix(Constants.SUFFIX_APPLIST_RINGTONE);
			context.write(outputKey, NullWritable.get());
		}
		AppRollingLog appRollingLog = new AppRollingLog();
		appRollingLog.setAppID(keyArr[0]);
		appRollingLog.setVersion(maxVersion);
		appRollingLog.setPlatform(keyArr[1]);
		appRollingLog.setChannel(currentChannel);
		appRollingLog.setGameServer(keyArr[2]);
		appRollingLog.setUid(keyArr[3]);
		appRollingLog.setAppList(appList);
		appRollingLog.setRingList(ringList);
		String[] rollingKey = appRollingLog.toStringArray();
		outputKey.setOutFields(rollingKey);
		outputKey.setSuffix(Constants.SUFFIX_APPLIST_ROLLING_DAY);
		context.write(outputKey, NullWritable.get());
	}

	/**
	 * 该设备有滚存信息，处理应用
	 * 
	 * @param key
	 * @param appRollingLog
	 * @param appList
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void oldApp(BigFieldsBaseModel key, AppRollingLog appRollingLog, List<AppDetail> appList, Context context,
			String maxVersion, String currentChannel) throws IOException, InterruptedException {
		String[] keyArr = key.getOutFields();
		// 上次上报的应用信息
		List<AppDetail> appListOld = appRollingLog.getAppList();
		String oldVersion = appRollingLog.getVersion();
		if (oldVersion.compareTo(maxVersion) == 1) {
			maxVersion = oldVersion;
			appRollingLog.setVersion(maxVersion);
		}
		// 遍历最新上报的列表，与滚存里面的进行对比，多出来的就是新增的
		for (AppDetail appDetail : appList) {
			// 上报的一个应用的信息
			String appName = appDetail.getAppName();
			int setupTimes = appDetail.getSetUpTimes();
			int upTime = appDetail.getUptime();
			// 遍历滚存
			boolean found = false;
			for (AppDetail appDetailOld : appListOld) {
				String appNameOld = appDetailOld.getAppName();
				// 如果是同一个应用=>安装量不用更新，启动次数、使用时长取最大值
				if (appName.equals(appNameOld)) {
					found = true;
					int setupTimesOld = appDetailOld.getSetUpTimes();
					int upTimeOld = appDetailOld.getUptime();
					// TODO:如果这个值能取到可能存在累计时间偏大的bug,应该取 两个数字的差的绝对值
					setupTimes = Math.max(setupTimes, setupTimesOld);
					upTime = Math.max(upTime, upTimeOld);
					String[] kArr = new String[] { keyArr[0], maxVersion, keyArr[1], currentChannel, keyArr[2],
							appName, "0", setupTimes + "", upTime + "" };
					outputKey.setOutFields(kArr);
					outputKey.setSuffix(Constants.SUFFIX_APPLIST_APP);
					context.write(outputKey, NullWritable.get());
				}
			}
			if (!found) {
				// 新安装的应用
				String[] kArr = new String[] { keyArr[0], maxVersion, keyArr[1], currentChannel, keyArr[2], appName,
						"1", setupTimes + "", upTime + "" };
				outputKey.setOutFields(kArr);
				outputKey.setSuffix(Constants.SUFFIX_APPLIST_APP);
				context.write(outputKey, NullWritable.get());
			}
		}
		// 反过来判断应用的卸载情况
		for (AppDetail appDetailOld : appListOld) {
			String appNameOld = appDetailOld.getAppName();
			boolean found = false;
			for (AppDetail appDetail : appList) {
				String appName = appDetail.getAppName();
				if (appNameOld.equals(appName)) {
					found = true;
				}
			}
			if (!found) {
				// 应用被卸载了
				String[] kArr = new String[] { keyArr[0], maxVersion, keyArr[1], currentChannel, keyArr[2], appNameOld,
						"-1", appDetailOld.getSetUpTimes() + "", appDetailOld.getUptime() + "" };
				outputKey.setOutFields(kArr);
				outputKey.setSuffix(Constants.SUFFIX_APPLIST_APP);
				context.write(outputKey, NullWritable.get());
			}
		}
	}

	/**
	 * 该设备已经有滚存信息，处理铃声
	 * 
	 * @param key
	 * @param appRollingLog
	 * @param ringList
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void oldRing(BigFieldsBaseModel key, AppRollingLog appRollingLog, List<AppDetail> ringList, Context context,
			String maxVersion, String currentChannel) throws IOException, InterruptedException {
		String[] keyArr = key.getOutFields();
		// 上次上报的铃声信息
		List<AppDetail> ringListOld = appRollingLog.getRingList();
		String oldVersion = appRollingLog.getVersion();
		if (oldVersion.compareTo(maxVersion) == 1) {
			maxVersion = oldVersion;
			appRollingLog.setVersion(maxVersion);
		}
		// 判断下今天新增加了哪些铃声
		for (AppDetail ringDetail : ringList) {
			// 上报的一个铃声的信息
			String ringName = ringDetail.getAppName();
			// 遍历滚存
			boolean found = false;
			for (AppDetail ringDetailOld : ringListOld) {
				String ringNameOld = ringDetailOld.getAppName();
				// 如果是同一个铃声
				if (ringName.equals(ringNameOld)) {
					found = true;
				}
			}
			if (!found) {
				// 新加了铃声
				String[] kArr = new String[] { keyArr[0], maxVersion, keyArr[1], currentChannel, keyArr[2], ringName,
						"1" };
				outputKey.setOutFields(kArr);
				outputKey.setSuffix(Constants.SUFFIX_APPLIST_RINGTONE);
				context.write(outputKey, NullWritable.get());
			}
		}
		// 反过来判断铃声的替换情况
		for (AppDetail ringOld : ringListOld) {
			String ringNameOld = ringOld.getAppName();
			boolean found = false;
			for (AppDetail ringDetail : ringList) {
				String ringName = ringDetail.getAppName();
				if (ringNameOld.equals(ringName)) {
					found = true;
				}
			}
			if (!found) {
				// 铃声被删除了
				String[] kArr = new String[] { keyArr[0], maxVersion, keyArr[1], currentChannel, keyArr[2],
						ringNameOld, "-1" };
				outputKey.setOutFields(kArr);
				outputKey.setSuffix(Constants.SUFFIX_APPLIST_RINGTONE);
				context.write(outputKey, NullWritable.get());
			}
		}
	}
}
