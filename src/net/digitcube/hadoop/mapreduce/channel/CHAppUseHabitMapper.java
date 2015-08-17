package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.EventLog2;
import net.digitcube.hadoop.model.channel.EventLog2.AttrKey;
import net.digitcube.hadoop.model.channel.HeaderLog2.ExtendKey;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 应用使用统计
 * 应用即资源，这里用Res表示
 * 
 * 
 * -----------------------------输入-----------------------------
 * 取自定义事件日志
 * 1.应用启动在线时长_DESelf_App_Launch
 * 2.应用安装_DESelf_App_Install
 * 3.应用卸载_DESelf_App_Uninstall
 * 
 * -----------------------------输出-----------------------------
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
 * 
 * @author sam.xie
 * @date 2015年3月4日 上午9:56:50
 * @version 1.0
 * 
 */
public class CHAppUseHabitMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog2 eventLog;
		try {
			// 日志里某些字段有换行字符，导致这条日志换行而不完整，
			// 用 try-catch 过滤掉这类型的错误日志
			eventLog = new EventLog2(arr);
		} catch (Exception e) {
			return;
		}

		OutFieldsBaseModel outputKey = new OutFieldsBaseModel();
		OutFieldsBaseModel outputValue = new OutFieldsBaseModel();

		String appID = eventLog.getAppID();
		// String appVersion = eventLog.getAppVersion();
		// String channel = eventLog.getChannel();
		String eventID = eventLog.getEventId();
		String country = eventLog.getExtendValue(ExtendKey.CNTY);
		String province = eventLog.getExtendValue(ExtendKey.PROV);
		String uid = eventLog.getUID();
		String packageName = eventLog.getAttrMap().get(AttrKey.PACKAGE_NAME);
		String resName = eventLog.getAttrMap().get(AttrKey.APP_NAME);
		String resVer = eventLog.getAttrMap().get(AttrKey.APP_VER);
		String[] keyFields = new String[] { appID, country, province, packageName };
		outputKey.setOutFields(keyFields);

		if (StringUtil.isEmpty(packageName)) { // 包名和版本名必填
			return;
		}
		if (StringUtil.isEmpty(resName)) {
			resName = "-";
		}
		if (StringUtil.isEmpty(resVer)) {
			resVer = "-";
		}
		/**
		 * 在Reducer阶段需要对活跃用户，启动次数，在线时长，安装次数，卸载次数统计为一条记录
		 */
		if (Constants.DESELF_APP_LAUNCH.equals(eventID)) {
			int duration = StringUtil.convertInt(eventLog.getAttrMap().get(AttrKey.DRUATION), 0);
			String launchTime = eventLog.getAttrMap().get(AttrKey.LAUNCH_TIME);
			if (StringUtil.isEmpty(launchTime)) {
				return;
			}
			if (duration > 86400) { // 在线时长超过1天，作为异常数据设置为0，有些会把时间戳直接作为在线时长上报
				duration = 0;
			}
			outputValue.setOutFields(new String[] { resName, resVer, launchTime, duration + "", uid });
			outputValue.setSuffix(Constants.SUFFIX_APP_LAUNCH);
			context.write(outputKey, outputValue);
		} else if (Constants.DESELF_APP_INSTALL.equals(eventID)) {
			String installTime = eventLog.getAttrMap().get(AttrKey.INSTALL_TIME);
			if (StringUtil.isEmpty(installTime))
				return;
			outputValue.setOutFields(new String[] { resName, resVer, installTime, uid });
			outputValue.setSuffix(Constants.SUFFIX_APP_INSTALL);
			context.write(outputKey, outputValue);
		} else if (Constants.DESELF_APP_UNINSTALL.equals(eventID)) {
			String uninstallTime = eventLog.getAttrMap().get(AttrKey.UNINSTALL_TIME);
			if (StringUtil.isEmpty(uninstallTime))
				return;
			outputValue.setOutFields(new String[] { resName, resVer, uninstallTime, uid });
			outputValue.setSuffix(Constants.SUFFIX_APP_UNINSTALL);
			context.write(outputKey, outputValue);
		}
	}
}
