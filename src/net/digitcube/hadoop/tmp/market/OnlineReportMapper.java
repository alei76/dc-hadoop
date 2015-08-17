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
public class OnlineReportMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	private Calendar cal = Calendar.getInstance();
	private String yyyyMMdd = "";
	//private String HH = "";
	static final String ONLINE_TIME = "ONLINE_TIME";
	static final String LOGIN_TIMES = "LOGIN_TIMES";
	static final String HOUR_OL_NUM = "HOUR_OL_NUM";
	
	//确保每玩家每小时只输出一次
	Set<String> hourSet = new HashSet<String>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Path inputPath = ((FileSplit)context.getInputSplit()).getPath();
		String fileName = inputPath.getName();
		
		//在输入路径中，从根目录开始，年份这个目录排在第几层
		//如 /data/digitcube/online_hour/2013/10/27/23/output 中年份排在第四
		//所以 year.index.in.path 的值应该设置为 4
		int yearIndex = context.getConfiguration().getInt("year.index.in.path", 4);
		String input = inputPath.toString();
		
		// 输入可能包含 'hdfs://'，将其去掉下面通过 '/' 分割是 year.index 才能正确
		input = input.replace("://",":"); 
		String[] components = input.split("/");
		
		//inputDirDate = yyyyMMdd
		yyyyMMdd = components[yearIndex]	//year
						 + components[yearIndex+1] //month
						 + components[yearIndex+2];//day
		//HH = components[yearIndex+3];
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(value.getLength() > 65536){ // >64KB
			return;
		}
		
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OnlineDayLog onlineDayLog = new OnlineDayLog(paraArr);
		
		//只统计  ALL_GS
		if(!MRConstants.ALL_GAMESERVER.equals(onlineDayLog.getExtend().getGameServer())){
			return;
		}
		int onlineTime = onlineDayLog.getTotalOnlineTime();
		int loginTimes = onlineDayLog.getTotalLoginTimes();
		String[] onlineRecords = onlineDayLog.getOnlineRecords().split(",");
		
		//online time
		String[] keyFields = new String[]{
				yyyyMMdd	
		};
		String[] valFields = new String[]{
				onlineDayLog.getAccountID(),
				onlineTime+""
		};
		keyObj.setSuffix(ONLINE_TIME);
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
		
		//login times
		valFields = new String[]{
				onlineDayLog.getAccountID(),
				loginTimes+""
		};
		keyObj.setSuffix(LOGIN_TIMES);
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
		
		//hour online num
		for(String onlineRecord : onlineRecords){
			String[] record = onlineRecord.split(":");
			if(record.length < 2){
				continue;
			}
			
			int loginTime = StringUtil.convertInt(record[0], 0);
			//不处理无效的数据
			if(loginTime <= 0){
				continue;
			}
			
			cal.setTimeInMillis(loginTime * 1000L);
			int hour = cal.get(Calendar.HOUR_OF_DAY); 
			
			keyFields = new String[]{
					hour+""	
			};
			valFields = new String[]{
					onlineDayLog.getAccountID()
			};
			keyObj.setSuffix(HOUR_OL_NUM);
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			context.write(keyObj, valObj);
		}
	}
}
