package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 用户流量-天
 * 取出 @EventSeparatorMapper 分离出来的 自定义事件  DESelf_APP_TRAFFIC
 * key:  AppID,Platform, Channel, GAMESERVER,UP|DOWN,NetType 
 * value: AccountID,Value
 * Title: NetworkTrafficDayMapper.java<br>
 * Description: NetworkTrafficDayMapper.java<br>
 * Copyright: Copyright (c) 2013 www.dataeye.com<br>
 * Company: DataEye 数据之眼<br>
 * 
 * @author Ivan     <br>
 * @date 2014-6-6         <br>
 * @version 1.0
 * <br>
 */
public class NetworkTrafficDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel outputKey = new OutFieldsBaseModel();
	private OutFieldsBaseModel outputValue = new OutFieldsBaseModel();

	//20150202：关于上下行流量大小，分别取 4kw 数据进行训练
	//得到结果为上下行流量取下列值时可以覆盖 99% 的数据
	//上行：[1, 5MB]
	//下行：[1, 600MB]
	private static final int UPLOADLINK_MAX = 5 * 1024 * 1024;
	private static final int DOWNLOADLINK_MAX = 600 * 1024 * 1024;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = new EventLog(arr);

		int uploadlink = StringUtil.convertInt(eventLog.getArrtMap().get("uplink"), 0);
		//20150202 ：上行流量大于 UPLOADLINK_MAX 视为异常数据丢弃
		if(uploadlink > UPLOADLINK_MAX){
			return;
		}
		
		String[] keyFields;
		String[] valueFields;
		// 处理上行流量的
		if (uploadlink > 0) {
			// 真实区服|真实网络类型
			keyFields = new String[] { eventLog.getAppID(), eventLog.getPlatform(), eventLog.getChannel(),
					eventLog.getGameServer(), Constants.NETWORK_TRAFFIC_UP, eventLog.getNetType() };
			valueFields = new String[] { eventLog.getUID(), uploadlink + "" };
			outputKey.setOutFields(keyFields);
			outputValue.setOutFields(valueFields);
			context.write(outputKey, outputValue);

			// 所有区服|真实网络类型
			keyFields[3] = MRConstants.ALL_GAMESERVER;
			outputKey.setOutFields(keyFields);
			context.write(outputKey, outputValue);

			// 真实区服|所有网络类型
			keyFields = new String[] { eventLog.getAppID(), eventLog.getPlatform(), eventLog.getChannel(),
					eventLog.getGameServer(), Constants.NETWORK_TRAFFIC_UP, Constants.NETTYPE_ALL_NT };
			valueFields = new String[] { eventLog.getUID(), uploadlink + "" };
			outputKey.setOutFields(keyFields);
			outputValue.setOutFields(valueFields);
			context.write(outputKey, outputValue);

			// 所有区服|所有网络类型
			keyFields[3] = MRConstants.ALL_GAMESERVER;
			outputKey.setOutFields(keyFields);
			context.write(outputKey, outputValue);

		}
		int downloadlink = StringUtil.convertInt(eventLog.getArrtMap().get("downlink"), 0);
		//20150202 ：下行流量大于 DOWNLOADLINK_MAX 视为异常数据丢弃
		if(downloadlink > DOWNLOADLINK_MAX){
			return;
		}
		
		// 处理下行流量的
		if (downloadlink > 0) {
			// 真实区服|真实网络类型
			keyFields = new String[] { eventLog.getAppID(), eventLog.getPlatform(), eventLog.getChannel(),
					eventLog.getGameServer(), Constants.NETWORK_TRAFFIC_DOWN, eventLog.getNetType() };
			outputKey.setOutFields(keyFields);
			valueFields = new String[] { eventLog.getUID(), downloadlink + "" };
			outputValue.setOutFields(valueFields);
			context.write(outputKey, outputValue);

			// 所有区服|真实网络类型
			keyFields[3] = MRConstants.ALL_GAMESERVER;
			outputKey.setOutFields(keyFields);
			context.write(outputKey, outputValue);
			// 真实区服|所有网络类型
			keyFields = new String[] { eventLog.getAppID(), eventLog.getPlatform(), eventLog.getChannel(),
					eventLog.getGameServer(), Constants.NETWORK_TRAFFIC_DOWN, Constants.NETTYPE_ALL_NT };
			outputKey.setOutFields(keyFields);
			valueFields = new String[] { eventLog.getUID(), downloadlink + "" };
			outputValue.setOutFields(valueFields);
			context.write(outputKey, outputValue);

			// 所有区服|所有网络类型
			keyFields[3] = MRConstants.ALL_GAMESERVER;
			outputKey.setOutFields(keyFields);
			context.write(outputKey, outputValue);
		}

	}
}
