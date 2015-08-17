package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * <pre>
 * 手机安装应用信息第二步
 * 
 *  @author Ivan     <br>
 *  @date 2014-6-10         <br>
 *  @version 1.0
 * <br>
 */
public class AppList2Mapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valueObj = new OutFieldsBaseModel();
	private String inputFileName;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// 获取文件名
		inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(MRConstants.SEPERATOR_IN);
		// 如果是铃声 appId, platForm, channel, gameRegion, appName , uidNumber
		// 在Reduce里面对uidNumber 求和
		if (inputFileName.endsWith(Constants.SUFFIX_APPLIST_RINGTONE)) {
			String[] keyArr = new String[] { values[0], values[1], values[2], values[3], values[4] };
			keyObj.setOutFields(keyArr);
			keyObj.setSuffix(Constants.SUFFIX_APPLIST_RINGTONE);
			String[] valueArr = new String[] { values[5] };
			valueObj.setOutFields(valueArr);
			context.write(keyObj, valueObj);
		}
		// appId, platForm, channel, gameRegion, appName, appVer, uid
		if (inputFileName.endsWith(Constants.SUFFIX_APPLIST_APP)) {
			String[] keyArr = new String[] { values[0], values[1], values[2], values[3], values[4] };
			keyObj.setOutFields(keyArr);
			keyObj.setSuffix(Constants.SUFFIX_APPLIST_APP);
			String[] valueArr = new String[] { "1", values[6], values[7] };
			valueObj.setOutFields(valueArr);
			context.write(keyObj, valueObj);
		}
	}
}
