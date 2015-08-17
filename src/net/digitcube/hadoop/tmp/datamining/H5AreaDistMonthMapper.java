package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.OnlineLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 统计每个月地区分布
 */
public class H5AreaDistMonthMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] onlineArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OnlineLog onlineLog = null;
		try {
			// 日志里某些字段有换行字符，导致这条日志换行而不完整，
			// 用 try-catch 过滤掉这类型的错误日志
			onlineLog = new OnlineLog(onlineArr);
		} catch (Exception e) {
			return;
		}
		String netType = onlineLog.getNetType();
		String country = onlineLog.getCountry();
		String province = onlineLog.getProvince();
		String accountId = onlineLog.getAccountID();
		
		keyObj.setOutFields(new String[] { netType });
		valObj.setOutFields(new String[] { accountId });
		keyObj.setSuffix("NET_DAU_DIST");
		context.write(keyObj, valObj);

		keyObj.setOutFields(new String[] { country, province });
		valObj.setOutFields(new String[] { accountId });
		keyObj.setSuffix("AREA_DAU_DIST");
		context.write(keyObj, valObj);

	}

}
