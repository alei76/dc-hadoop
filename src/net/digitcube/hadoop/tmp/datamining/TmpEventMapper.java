package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.FieldValidationUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TmpEventMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private String appIdFilter = "4A3588E87A7F7D0768669ADFA32ED630";// 《hello kitty 快乐消》灰度版
	private String eventIdFilter = "loading完成次数统计";
	private String versionFilter = "1.2";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		appIdFilter = context.getConfiguration().get("appIdFilter", appIdFilter);
		versionFilter = context.getConfiguration().get("versionFilter", versionFilter);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramArr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = null;
		try {
			eventLog = new EventLog(paramArr);
			if (!FieldValidationUtil.validateAppIdLength(eventLog.getAppID())) {
				return;
			}
			String[] appIdVersion = eventLog.getAppID().split("\\|");
			String appId = appIdVersion[0];
			String version = appIdVersion[1];
			if (appIdVersion.length >= 2) {
				version = appIdVersion[1];
			}

			if (appIdFilter.contains(appId) && versionFilter.contains(version) && eventIdFilter.contains(eventLog.getEventId())) {
				keyFields.setOutFields(paramArr);
				context.write(keyFields, NullWritable.get());
			}

		} catch (Exception e) {

		}
	}
}
