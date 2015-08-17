package net.digitcube.hadoop.mapreduce.adtrack;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.ClickLog;
import net.digitcube.hadoop.model.UserInfoLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ClickAndActiveMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	// 当前输入的文件后缀
	private String fileSuffix = "";

	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}


	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] keyFields = null;
		String[] valueFields = null;
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		
		//输入文件名：UserInfo-yyyy-MM-dd-HH.log_dcserver1
		if (fileSuffix.contains(Constants.SUFFIX_USERINFO_HOUR)) {
			UserInfoLog userInfoLog = new UserInfoLog(paraArr);
			if (Constants.Channle_For_Track.equals(userInfoLog.getChannel())
					&& userInfoLog.getActTime() > 0) { // 需要track的渠道，且设备处于刚激活状态
				String[] appInfo = userInfoLog.getAppID().split("\\|");
				keyFields = new String[] { appInfo[0], userInfoLog.getBrand(),
						userInfoLog.getOperSystem() };
				// 0 U
				// 1 actTime
				// 2 IP
				// 3 设备型号
				// 4 操作系统版本号
				// 5 UID
				valueFields = new String[] { "U",
						userInfoLog.getActTime() + "",
						userInfoLog.getExtend_4(), userInfoLog.getProvince(),
						userInfoLog.getOperators(), userInfoLog.getUID() };

			}

		} else if (fileSuffix.contains(Constants.SUFFIX_CLICK_HOUR)) {
			ClickLog clickLog = new ClickLog(paraArr);
			if (clickLog.isIOS()) {
				keyFields = new String[] { clickLog.getAppid(),
						clickLog.getDevice(), clickLog.getOs() };
				// 0 C
				// 1 timestamp
				// 2 IP
				// 3 设备型号
				// 4 操作系统版本号
				// 5 渠道
				valueFields = new String[] { "C", clickLog.getTimestamp(),
						clickLog.getIp(), clickLog.getProvince(),
						clickLog.getOperator(), clickLog.getChannel() };
			}
		}
		if (keyFields != null && valueFields != null) {
			mapKeyObj.setOutFields(keyFields);
			mapValueObj.setOutFields(valueFields);
			context.write(mapKeyObj, mapValueObj);
		}
	}
}
