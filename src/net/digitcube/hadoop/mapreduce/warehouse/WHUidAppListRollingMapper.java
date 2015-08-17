package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.AppListLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <pre/>
 * 设备应用列表
 * 
 * 一点说明：在应用商店启动时候，sdk会上报当前设备上所有的应用列表
 * 在logserver阶段，将每个应用都拆分成一条日志。按当天作为最后上报时间
 * 
 * 输入：
 * 1.应用详情日志AppDetail @see AppDetail
 * 
 * 2.应用列表历史滚存
 * uid	lastUpdateTime	lastAppList[appName,appName,...,appName]	pastAppList[appName,appName,...,appName]
 * 
 * 输出：
 * 1.uid上最近一次上报的时间，最近一次上报的应用列表，和曾经安装过的应用列表
 * uid	lastUploadTime	lastAppList[appName,appName,...,appName]	pastAppList[appName,appName,...,appName]
 * 
 * @author sam.xie
 * @date 2015年8月6日 下午3:25:14
 * @version 1.0
 */
public class WHUidAppListRollingMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private String fileSuffix = null;
	private static int MAX_LENGTH = 32768; // 理论最大值65536
	private static final String UTF8 = "UTF-8";
	private static final Calendar cal = Calendar.getInstance();

	@Override
	protected void setup(Context context) {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);

		if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_APPLIST_ROLLING)) {
			String uid = paramsArr[0];
			String lastUpdateTime = paramsArr[1];
			String lastAppList = paramsArr[2]; // 最后一次上报的应用列表
			String pastAppList = paramsArr[3]; // 历史上有上报的应用列表
			lastAppList = StringUtil.isEmpty(lastAppList) || lastAppList.getBytes(UTF8).length > MAX_LENGTH ? "-" : lastAppList;
			pastAppList = StringUtil.isEmpty(pastAppList) || pastAppList.getBytes(UTF8).length > MAX_LENGTH ? "-" : pastAppList;
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_APPLIST_ROLLING);
			keyFields.setOutFields(new String[] { uid });
			valFields.setSuffix("R");
			valFields.setOutFields(new String[] { lastUpdateTime, lastAppList, pastAppList });
			try {
				context.write(keyFields, valFields);
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("key:" + Arrays.toString(keyFields.getOutFields()) + ",value:"
						+ Arrays.toString(valFields.getOutFields()));
			}
		} else if (fileSuffix.contains("AppDetail")) {
			AppListLog log = null;
			try {
				log = new AppListLog(paramsArr);
			} catch (Exception e) {
				return;
			}
			String uid = log.getUID();
			String ts = log.getTimestamp();
			ts = ts.length() > 10 ? ts.substring(0, 10) : ts;// // 毫秒时间戳转为秒时间戳，这里只保留高10位
			ts = truncateTime(ts);
			String pkgName = log.getPackageName();
			pkgName = StringUtil.isEmpty(pkgName) || pkgName.getBytes(UTF8).length > MAX_LENGTH ? "-" : pkgName;
			String appName = log.getAppName();
			appName = StringUtil.isEmpty(appName) || appName.getBytes(UTF8).length > MAX_LENGTH ? pkgName : appName; // 如果appName不合法，就取pkgName
			if ("-".equals(appName)) {
				return;
			}
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_APPLIST_ROLLING);
			keyFields.setOutFields(new String[] { uid });
			valFields.setSuffix("D");
			valFields.setOutFields(new String[] { ts, appName });
			try {
				context.write(keyFields, valFields);
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("key:" + Arrays.toString(keyFields.getOutFields()) + ",value:"
						+ Arrays.toString(valFields.getOutFields()));
			}
		}
	}

	private String truncateTime(String unixtime) {
		long ts = StringUtil.convertInt(unixtime, 0);
		cal.setTimeInMillis(ts * 1000);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return (int) (cal.getTimeInMillis() / 1000) + "";
	}

}
