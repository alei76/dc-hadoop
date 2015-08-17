package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LiuLiangMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {

	private String fileSuffix;
	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private IntWritable times = new IntWritable(1);

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		keyFields.setOutFields(new String[1]);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if (fileSuffix.contains(Constants.EVENT_NETWORK_TRAFFICE)) {
			// 从原始EventSelf 日志里面去读取上个小时的日志
			String[] valuesArr = value.toString().split(MRConstants.SEPERATOR_IN);
			EventLog eventLog = null;
			try {
				eventLog = new EventLog(valuesArr);
			} catch (Exception e) {
				return;
			}
			
			//验证appId长度 并修正appId  add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(eventLog.getAppID())){
				return;
			}
			
			int uploadlink = StringUtil.convertInt(eventLog.getArrtMap().get("uplink"), 0);
			int downloadlink = StringUtil.convertInt(eventLog.getArrtMap().get("downlink"), 0);
			
			//上行
			if(uploadlink>0){
				keyFields.getOutFields()[0] = uploadlink+"";
				keyFields.setSuffix("UPLOADLINK");
				context.write(keyFields, times);
			}
			//下行
			if(downloadlink>0){
				keyFields.getOutFields()[0] = downloadlink+"";
				keyFields.setSuffix("DOWNLOADLINK");
				context.write(keyFields, times);
			}
		}
	}
}
