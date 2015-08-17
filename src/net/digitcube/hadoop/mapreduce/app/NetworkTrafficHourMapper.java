package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 计算每个小时 用户的流量情况
 * 依赖上个小时的（后缀为 _DESelf_APP_TRAFFIC）
 * 
 * key:   AppID,Platform, Channel, GAMESERVER,nwtf,UP|DOWN,NetType 
 * value: AccountID,Value
 * 
 * Title: NetworkTrafficHourMapper.java<br>
 * Description: NetworkTrafficHourMapper.java<br>
 * Copyright: Copyright (c) 2013 www.dataeye.com<br>
 * Company: DataEye 数据之眼<br>
 * 
 * @author Ivan <br>
 * @date 2014-6-6 <br>
 * @version 1.0
 * <br>
 */
public class NetworkTrafficHourMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel outputKey = new OutFieldsBaseModel();
	private OutFieldsBaseModel outputValue = new OutFieldsBaseModel();
	
	//20150202：关于上下行流量大小，分别取 4kw 数据进行训练
	//得到结果为上下行流量取下列值时可以覆盖 99% 的数据
	//而其它范围内数据视为异常值
	//上行：[1, 5MB]
	//下行：[1, 600MB]
	private static final int UPLOADLINK_MAX = 5 * 1024 * 1024;
	private static final int DOWNLOADLINK_MAX = 600 * 1024 * 1024;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 从原始EventSelf 日志里面去读取上个小时的日志
		String[] valuesArr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = null;
		try {
			eventLog = new EventLog(valuesArr);
		} catch (Exception e) {
			return;
		}
		if (eventLog == null) {
			return;
		}
		
		//验证appId长度 并修正appId  add by mikefeng 20141010
		if(!FieldValidationUtil.validateAppIdLength(eventLog.getAppID())){
			return;
		}
		
		String eventID = eventLog.getEventId();
		eventID = eventID.contains(":") ? eventID.split(":")[0] : eventID;
		eventID = eventID.startsWith("_") ? eventID.replaceFirst("_", "") : eventID;
		// 找到与网络流量相关的记录
		if (eventID.equals(Constants.EVENT_NETWORK_TRAFFICE)) {
			int uploadlink = StringUtil.convertInt(eventLog.getArrtMap().get("uplink"), 0);
			//20150202 ：上行流量大于 UPLOADLINK_MAX 视为异常数据丢弃
			if(uploadlink > UPLOADLINK_MAX){
				return;
			}
			String[] keyFields;
			String[] valueFields;
			if (uploadlink > 0) {
				// 处理上行流量的
				// real gameregion UP
				keyFields = new String[] { eventLog.getAppID(), eventLog.getPlatform(), eventLog.getChannel(),
						eventLog.getGameServer(), Constants.NETWORK_TRAFFIC, Constants.NETWORK_TRAFFIC_UP,
						eventLog.getNetType() };
				valueFields = new String[] { eventLog.getUID(), uploadlink + "" };
				outputKey.setOutFields(keyFields);
				outputValue.setOutFields(valueFields);
				context.write(outputKey, outputValue);

				// all gameregion UP
				keyFields[3] = MRConstants.ALL_GAMESERVER;
				outputKey.setOutFields(keyFields);
				context.write(outputKey, outputValue);
			}
			int downloadlink = StringUtil.convertInt(eventLog.getArrtMap().get("downlink"), 0);
			//20150202 ：下行流量大于 DOWNLOADLINK_MAX 视为异常数据丢弃
			if(downloadlink > DOWNLOADLINK_MAX){
				return;
			}
			if (downloadlink > 0) {
				// 处理下行流量的
				// real gameregion DOWN
				keyFields = new String[] { eventLog.getAppID(), eventLog.getPlatform(), eventLog.getChannel(),
						eventLog.getGameServer(), Constants.NETWORK_TRAFFIC, Constants.NETWORK_TRAFFIC_DOWN,
						eventLog.getNetType() };
				outputKey.setOutFields(keyFields);
				valueFields = new String[] { eventLog.getUID(), downloadlink + "" };
				outputValue.setOutFields(valueFields);
				context.write(outputKey, outputValue);
				// all gameregion DOWN
				keyFields[3] = MRConstants.ALL_GAMESERVER;
				outputKey.setOutFields(keyFields);
				context.write(outputKey, outputValue);
			}
		}
	}
}
