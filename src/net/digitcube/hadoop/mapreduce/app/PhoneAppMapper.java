package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.AppRollingLog;
import net.digitcube.hadoop.model.AppListLog;
import net.digitcube.hadoop.util.FieldValidationUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PhoneAppMapper extends Mapper<LongWritable, Text, BigFieldsBaseModel, BigFieldsBaseModel> {

	private String fileSuffix;
	private BigFieldsBaseModel outputKey = new BigFieldsBaseModel();
	private BigFieldsBaseModel outputValue = new BigFieldsBaseModel();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lineArr = value.toString().split(MRConstants.SEPERATOR_IN);
		if (lineArr.length == 8) {
			if (lineArr[6].length() > 65535) {
				return;
			}
			if (lineArr[7].length() > 65535) {
				return;
			}
		}
		// 有两种输入文件
		// 1.当前天或者小时的 appDetail日志
		// 2.APP_DETAIL_ROLLING_DAY
		if (fileSuffix.endsWith(Constants.SUFFIX_APPLIST_ROLLING_DAY)) {
			// 是滚存日志 输出格式 appID version platform channel gameserver uid applist ringlist
			AppRollingLog appRollingLog = null;
			try{
				appRollingLog = new AppRollingLog(lineArr);
			}catch(Exception e){
				e.printStackTrace();
				return;
			}
			String[] keyArr = new String[] { appRollingLog.getAppID(), appRollingLog.getPlatform(),
					appRollingLog.getGameServer(), appRollingLog.getUid() };
			outputKey.setOutFields(keyArr);

			outputValue.setOutFields(lineArr);
			outputValue.setSuffix("R");
			context.write(outputKey, outputValue);
		}
		if (fileSuffix.contains("AppDetail")) {
			AppListLog appListLog = null;
			try{
				appListLog = new AppListLog(lineArr);
			}catch(Exception e){
				e.printStackTrace();
				return;
			}
			//验证appId长度 并修正appId  add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(appListLog.getAppID())){
				return;
			}
			
			String appInfo = appListLog.getAppID();
			String[] appArr = appInfo.split("\\|");
			String appID = appArr[0];
			String version = appArr[1];
			String platForm = appListLog.getPlatform();
			String channel = appListLog.getChannel();
			String gameRegion = appListLog.getGameServer();
			String uid = appListLog.getUID();

			String packageName = appListLog.getPackageName();
			String appName = appListLog.getAppName();
			String appVer = appListLog.getAppVer();
			String installTime = appListLog.getInstallTime();
			String lastAccessTime = appListLog.getLastAccessTime();
			String setupTimes = appListLog.getSetUpTimes() + "";
			String upTime = appListLog.getUpTime() + "";
			String ext = appListLog.getExt();

			if (packageName.equals(Constants.APPLIST_RINGTONE)) {

				// 铃声，只需要取到铃声名字就可以了
				String[] keyArr = new String[] { appID, platForm, gameRegion, uid };
				outputKey.setOutFields(keyArr);
				String[] valueArr = new String[] { packageName, appName, appVer, installTime, lastAccessTime,
						setupTimes, upTime, ext, version, channel };
				outputValue.setOutFields(valueArr);
				outputValue.setSuffix(Constants.SUFFIX_APPLIST_RINGTONE);
				context.write(outputKey, outputValue);
				keyArr[2] = MRConstants.ALL_GAMESERVER;
				outputKey.setOutFields(keyArr);
				context.write(outputKey, outputValue);
			} else {
				// 应用，取出应用的版本、启动次数、使用时长
				String[] keyArr = new String[] { appID, platForm, gameRegion, uid };
				outputKey.setOutFields(keyArr);
				String[] valueArr = new String[] { packageName, appName, appVer, installTime, lastAccessTime,
						setupTimes, upTime, ext, version, channel };
				outputValue.setOutFields(valueArr);
				outputValue.setSuffix(Constants.SUFFIX_APPLIST_APP);
				context.write(outputKey, outputValue);
				keyArr[2] = MRConstants.ALL_GAMESERVER;
				outputKey.setOutFields(keyArr);
				context.write(outputKey, outputValue);
			}
		}
	}
}
