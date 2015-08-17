package net.digitcube.hadoop.tmp.market;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.UserInfoLog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jruby.runtime.Constants;

/**
 * 主要逻辑：
 * 玩家设备TOP10机型
 * 玩家设备分辨率分布
 * 玩家设备操作系统分布
 * 玩家游戏联网方式分布
 * 玩家设备运营商分布
 */
public class DeviceReportMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private IntWritable valObj = new IntWritable(1);
	
	static final String SUFFIX_BRAND = "BRAND";
	static final String SUFFIX_RESOL = "RESOL";
	static final String SUFFIX_OS_iOS = "OS_iOS";
	static final String SUFFIX_OS_ANDR = "OS_ANDR";
	static final String SUFFIX_OS_WIN = "OS_WIN";
	static final String SUFFIX_OS_OTHER = "OS_OTHER";
	static final String SUFFIX_NETTYPE = "NETTYPE";
	static final String SUFFIX_OPERATOR = "OPERATOR";
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] userInfoArr = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoLog userInfoLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			userInfoLog = new UserInfoLog(userInfoArr);
		}catch(Exception e){
			//TODO do something to mark the error here
			return;
		}
		
		//只统计  ALL_GS
		if(!MRConstants.ALL_GAMESERVER.equals(userInfoLog.getGameServer())){
			return;
		}
				
		//只取激活统计
		if(userInfoLog.getActTime() < 0 ){
			return;
		}
		
		String platform = userInfoLog.getPlatform();
		String brand = userInfoLog.getBrand();
		String resol = userInfoLog.getResolution();
		String OS = userInfoLog.getOperSystem();
		String netType = userInfoLog.getNetType();
		String operator = userInfoLog.getOperators();
		
		//品牌
		String[] keyFields = new String[]{brand};
		keyObj.setOutFields(keyFields);
		keyObj.setSuffix(SUFFIX_BRAND);
		context.write(keyObj, valObj);
		
		//分辨率
		keyFields = new String[]{resol};
		keyObj.setOutFields(keyFields);
		keyObj.setSuffix(SUFFIX_RESOL);
		context.write(keyObj, valObj);
		
		//操作系统
		String os_suffix = SUFFIX_OS_OTHER;
		if("1".equals(platform)){
			os_suffix = SUFFIX_OS_iOS;
		}else if("2".equals(platform)){
			os_suffix = SUFFIX_OS_ANDR;
		}else if("2".equals(platform)){
			os_suffix = SUFFIX_OS_WIN;
		}
		keyFields = new String[]{OS};
		keyObj.setOutFields(keyFields);
		keyObj.setSuffix(os_suffix);
		context.write(keyObj, valObj);
		
		//网络方式
		keyFields = new String[]{netType};
		keyObj.setOutFields(keyFields);
		keyObj.setSuffix(SUFFIX_NETTYPE);
		context.write(keyObj, valObj);
		
		//运营商
		keyFields = new String[]{operator};
		keyObj.setOutFields(keyFields);
		keyObj.setSuffix(SUFFIX_OPERATOR);
		context.write(keyObj, valObj);
	}
}
