package net.digitcube.hadoop.tmp;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 比较两个滚存文件中UID
 * /data/warehouse/online_rolling/2015/07/25/00/output/*WAREHOUSE_DEVICE_ROLLING
 * /data/warehouse/uid_rolling/2015/07/25/00/output/*WAREHOUSE_UID_ROLLING
 * 
 * 输出差异文件
 */
public class CompareWHUidAndWHOnlineMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	private String fileSuffix = "";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramArr = value.toString().split(MRConstants.SEPERATOR_IN);
		if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_UID_ROLLING)) {
			String uid = paramArr[0];
			mapKeyObj.setOutFields(new String[] { uid });
			mapValObj.setSuffix("UID");
			mapValObj.setOutFields(paramArr);
			context.write(mapKeyObj, mapValObj);
		} else if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_DEVICE_ROLLING)) {
			String uid = paramArr[0];
			mapKeyObj.setOutFields(new String[] { uid });
			mapValObj.setSuffix("DEV");
			mapValObj.setOutFields(paramArr);
			context.write(mapKeyObj, mapValObj);
		}
	}
}
