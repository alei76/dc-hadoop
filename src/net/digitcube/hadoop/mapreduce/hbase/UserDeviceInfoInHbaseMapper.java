package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.util.FieldValidationUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserDeviceInfoInHbaseMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] onlineArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OnlineLog onlineLog = null;
		try {
			// 日志里某些字段有换行字符，导致这条日志换行而不完整，
			// 用 try-catch 过滤掉这类型的错误日志
			onlineLog = new OnlineLog(onlineArr);
		} catch (Exception e) {
			// TODO do something to mark the error here
			return;
		}
		
		//验证appId长度 并修正appId  add by mikefeng 20141010
		if(!FieldValidationUtil.validateAppIdLength(onlineLog.getAppID())){
			return;
		}
		
		String realAppid = onlineLog.getAppID().split("\\|")[0];
		mapKeyObj.setOutFields(new String[] { realAppid,
				onlineLog.getPlatform(), onlineLog.getAccountID(),
				onlineLog.getUID() });
		String[] valFields = new String[] { onlineLog.getLoginTime() + "",
				onlineLog.getAccountType(), onlineLog.getGender(),
				onlineLog.getAge(), onlineLog.getResolution(),
				onlineLog.getOperSystem(), onlineLog.getBrand(),
				onlineLog.getNetType(), onlineLog.getCountry(),
				onlineLog.getProvince(), onlineLog.getOperators() };

		mapValueObj.setOutFields(valFields);
		context.write(mapKeyObj, mapValueObj);
	}
}
