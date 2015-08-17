package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.OnlineLog2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 输入：当天 24 个时间片的在线日志
 * 主要逻辑： 按 UID 进行去重
 * a)统计登录次数及在线时长，记录每次登录
 * b)国家/地区/机型等类型属性值去最早一次登录的值
 */
public class CHOnlineDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] onlineArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OnlineLog2 onlineLog2 = null;
		try{
			onlineLog2 = new OnlineLog2(onlineArr);
		}catch(Exception e){
			return;
		}
		
		String[] keyFields = new String[]{
				onlineLog2.getAppID(),
				onlineLog2.getPlatform(),
				onlineLog2.getUID()
		};
		
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(onlineArr);
		
		context.write(keyObj, valObj);
	}
}
