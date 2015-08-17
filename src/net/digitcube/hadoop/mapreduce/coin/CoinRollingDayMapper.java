package net.digitcube.hadoop.mapreduce.coin;

import java.io.IOException;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.CommonExtend;
import net.digitcube.hadoop.mapreduce.domain.CommonHeader;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.mapreduce.domain.UserInfoLog;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.model.EventLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 虚拟币滚存信息，计算 APP 虚拟币总存量
 */
public class CoinRollingDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	private String fileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		
		String[] keyFields = null;
		String[] valueFields = null;
		String[] keyFields_AllGS = null;
		if(fileName.contains(Constants.SUFFIX_COIN_ROLLING)){
			if(array.length < 8){
				return;
			}
			
			int i = 0;
			String[] appInfo = array[i++].split("\\|");
			String platform = array[i++];
			String channel = array[i++];
			String gameServer = array[i++];
			String accountID = array[i++];
			String coinType = Constants.DEFAULT_COIN_TYPE;
			if(array.length > 7){//20140617 : 加入 coinType，新旧数据需兼容
				coinType = array[i++];
			}
			String coinNum = array[i++];
			String lastUpdateTime = array[i++];
			
			keyFields = new String[]{appInfo[0], platform, channel, gameServer, accountID, coinType};
			valueFields = new String[]{"A", appInfo[1], coinNum, lastUpdateTime};
			
		}else if(fileName.contains(Constants.DESelf_Coin_Num)){
			
			EventLog eventLog = new EventLog(array);
			String[] appInfo = eventLog.getAppID().split("\\|");
			String accountID = eventLog.getAccountID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer = eventLog.getGameServer();
			String coinType = eventLog.getArrtMap().get("coinType");
			String seq = eventLog.getArrtMap().get("seq");
			String coinNum = eventLog.getArrtMap().get("total");
			//20140402：当前 SDK 中存在上报事件中必须有属性不存在问题
			//这些属性是用于逻辑计算的，如果不存在则过滤该记录
			if(null == seq || null == coinNum){
				return;
			}
			//20140617 : coinType 在之前的协议中没有，这里需做兼容
			coinType = null == coinType ? Constants.DEFAULT_COIN_TYPE : coinType;
			
			//真实区服
			keyFields = new String[]{appInfo[0], platform, channel, gameServer, accountID, coinType};
			valueFields = new String[]{"B", appInfo[1], coinNum, seq};
			
			//全服
			keyFields_AllGS = new String[]{appInfo[0], platform, channel, MRConstants.ALL_GAMESERVER, accountID, coinType};
		}

		//真实区服
		if (null != keyFields) {
			mapKeyObj.setOutFields(keyFields);
			mapValueObj.setOutFields(valueFields);
			context.write(mapKeyObj, mapValueObj);
		}
		
		//全服
		if(null != keyFields_AllGS){
			mapKeyObj.setOutFields(keyFields_AllGS);
			mapValueObj.setOutFields(valueFields);
			context.write(mapKeyObj, mapValueObj);
		}
	}
}
