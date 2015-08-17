package net.digitcube.hadoop.mapreduce.coin;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <pre>
 * 当天消费虚拟币的玩家数量去重 Map
 * Title: CoinUserDureMapper.java<br>
 * Description: CoinUserDureMapper.java<br>
 * Copyright: Copyright (c) 2013 www.dataeye.com<br>
 * Company: DataEye 数据之眼<br>
 * 
 * @author mikefeng     <br>
 * @date 2014-8-19         <br>
 * @version 1.0
 * <br>
 * 
 * 输入文件： 
 * 后缀为 DESelf_Coin_Lost 虚拟币消耗   
 * 
 * Map 输出：
 * [appID, platform, channel, gameServer,coinType,accountId ]
 * 
 * Reduce 输出
 * [appID, platform, channel, gameServer, accountId, coinType]
 */

public class CoinUserDureMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
			
		EventLog eventLog = new EventLog(paraArr);
		String appID = eventLog.getAppID();
		String platform = eventLog.getPlatform();
		String channel = eventLog.getChannel();
		String gameServer = eventLog.getGameServer();
		String accountId = eventLog.getAccountID();
		
		//20140617 : coinType 在之前的协议中没有，这里需做兼容
		String coinType = eventLog.getArrtMap().get("coinType");
		coinType = null == coinType ? Constants.DEFAULT_COIN_TYPE : coinType;
		
		//真实区服
		String[] keyFields = new String[] { appID, platform, channel, gameServer, accountId, coinType };		
		
		keyObj.setOutFields(keyFields);
		keyObj.setSuffix(Constants.SUFFIX_COIN_GAIN_LOST_USERDURE);
		
		context.write(keyObj, NullWritable.get());
	}
	
}
