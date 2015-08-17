package net.digitcube.hadoop.tmp.market;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.model.UserInfoLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 主要逻辑：
 * 玩家设备TOP10机型
 * 玩家设备分辨率分布
 * 玩家设备操作系统分布
 * 玩家游戏联网方式分布
 * 玩家设备运营商分布
 */
public class OnlineReportSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	private String fileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Path inputPath = ((FileSplit)context.getInputSplit()).getPath();
		fileName = inputPath.getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String yyyyMMdd = paraArr[i++];
		String totalPlayerCount = paraArr[i++];
		String totalSum = paraArr[i++];
		String avgSum = paraArr[i++];
		if(fileName.endsWith(OnlineReportMapper.ONLINE_TIME)){
			keyObj.setSuffix(OnlineReportMapper.ONLINE_TIME);
			valObj.setOutFields(new String[]{avgSum});
			context.write(keyObj, valObj);
			
		}else if(fileName.endsWith(OnlineReportMapper.LOGIN_TIMES)){
			keyObj.setSuffix(OnlineReportMapper.LOGIN_TIMES);
			valObj.setOutFields(new String[]{avgSum});
			context.write(keyObj, valObj);
			
		}else if(fileName.endsWith(OnlineReportMapper.HOUR_OL_NUM)){
			String hour = yyyyMMdd;
			keyObj.setSuffix(OnlineReportMapper.HOUR_OL_NUM);
			valObj.setOutFields(new String[]{totalPlayerCount,hour});
			context.write(keyObj, valObj);
		}
	}
}
