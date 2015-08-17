package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.AppListLog;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 应用列表
 * 第一步 ： 对同一个设备UID 上报上来的同一个appName+appVer 取最大启动次数、最大在线时间
 * 日志：
 * Timestamp,SDKVersion,APPID| APPVERSION,UID,AccountID,Platform,Packagename,appName,AppVer,InstallTime,LastAccessTime,SetUpTImes,UpTime,Ext
 * 
 *   
 * @author Ivan     <br>
 * @date 2014-6-9         <br>
 * @version 1.0
 * <br>
 */

public class AppListMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] appArr = value.toString().split(MRConstants.SEPERATOR_IN);
		// Timestamp,SDKVersion,APPID| APPVERSION,UID,AccountID,Platform,Packagename,
		// appName,AppVer,InstallTime,LastAccessTime,SetUpTImes,UpTime,Ext
		AppListLog appListLog = new AppListLog(appArr);

		String appId = appListLog.getAppID();
		String uid = appListLog.getUID();
		String platForm = appListLog.getPlatform();
		String channel = appListLog.getChannel();
		String gameRegion = appListLog.getGameServer();
		String packageName = appListLog.getPackageName();
		String appName = appListLog.getAppName();
		if (packageName.equals(Constants.APPLIST_RINGTONE)) {
			// 铃声，只需要取到铃声名字就可以了
			String[] keyArr = new String[] { appId, platForm, channel, gameRegion, appName };
			mapKeyObj.setOutFields(keyArr);
			mapKeyObj.setSuffix(Constants.SUFFIX_APPLIST_RINGTONE);
			String[] valueArr = new String[] { uid };
			mapValObj.setOutFields(valueArr);
			context.write(mapKeyObj, mapValObj);

			keyArr[3] = MRConstants.ALL_GAMESERVER;
			mapKeyObj.setOutFields(keyArr);
			context.write(mapKeyObj, mapValObj);
		} else {
			// 应用，取出应用的版本、启动次数、使用时长

			String appVer = appListLog.getAppVer();
			String setupTimes = appListLog.getSetUpTimes() + "";
			String upTime = appListLog.getUpTime() + "";
			String[] keyArr = new String[] { appId, platForm, channel, gameRegion, appName, appVer, uid };
			mapKeyObj.setOutFields(keyArr);
			mapKeyObj.setSuffix(Constants.SUFFIX_APPLIST_APP);
			String[] valueArr = new String[] { setupTimes, upTime };
			mapValObj.setOutFields(valueArr);
			context.write(mapKeyObj, mapValObj);
			keyArr[3] = MRConstants.ALL_GAMESERVER;
			mapKeyObj.setOutFields(keyArr);
			context.write(mapKeyObj, mapValObj);
		}
	}
}
