package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 主要逻辑
 * 对每个道具当天的购买情况进行汇总
 * 
 * 输入：APPID,Platform,GameServer,AccountID,ItemId,ItemType
 * 		 channel,[currencyType:sum(itemCnt)|sum(vituralCurrency),...]
 * Map:
 * key：
 * 		APPID,Platform,Channel,GameServer,ItemId,ItemType
 * value：
 * 		[currencyType:sum(itemCnt)|sum(vituralCurrency)...]
 * 
 * Reduce:
 * 		APPID,Platform,Channel,GameServer,ItemId,ItemType,type(道具购买),vkey,totalPlayer(总购买人数)
 * 		APPID,Platform,Channel,GameServer,ItemId,ItemType,type(道具购买数量),currencyType,itemCnt(当前币种购买数量)
 * 		APPID,Platform,Channel,GameServer,ItemId,ItemType,type(道具购买金额),currencyType,vituralCurrency(当前币种购买总金额)
 */

public class ItemBuySumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, Text> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private Text mapValObj = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try{
			
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		String appID = array[0];
		String platform = array[1];
		String gameServer = array[2];
		String accountId = array[3];
		String playerType = array[4];
		String channel = array[5];
		String itemRedords = array[6];
		
		String[] playerTypeArr = null;
		if(Constants.DATA_FLAG_PLAYER_NEW_ONLINE_PAY.equals(playerType)){
			playerTypeArr = new String[]{
					Constants.PLAYER_TYPE_NEWADD, //新增
					Constants.PLAYER_TYPE_ONLINE, //活跃
					Constants.PLAYER_TYPE_PAYMENT //付费
			};
		}else if(Constants.DATA_FLAG_PLAYER_NEW_ONLINE.equals(playerType)){
			playerTypeArr = new String[]{
					Constants.PLAYER_TYPE_NEWADD, //新增
					Constants.PLAYER_TYPE_ONLINE  //活跃
			};
		}else if(Constants.DATA_FLAG_PLAYER_ONLINE_PAY.equals(playerType)){
			playerTypeArr = new String[]{
					Constants.PLAYER_TYPE_ONLINE, //活跃
					Constants.PLAYER_TYPE_PAYMENT //付费
			};
		}else if(Constants.DATA_FLAG_PLAYER_ONLINE.equals(playerType)){
			playerTypeArr = new String[]{
					Constants.PLAYER_TYPE_ONLINE //活跃
			};
		}
		if(null == playerTypeArr){
			return;
		}
		
		// 取出 itemId  -->  curType:sum
		Map<String, String> virCurrencyMap = new HashMap<String, String>();
		String[] groupArray = StringUtil.split(itemRedords,",");	
		
		for(String groupCurrTypeAndVals : groupArray){		
			String[] currTypeAndVal = StringUtil.split(groupCurrTypeAndVals,":");
			if(currTypeAndVal.length < 2){
				continue;
			}
			String csum = currTypeAndVal[1];
			
			String[] groupKey = StringUtil.split(currTypeAndVal[0], "|");
			String itemId = groupKey[0];
			String itemType = groupKey[1];
			String currType = groupKey[2];
			String  itemsum = virCurrencyMap.get(itemId + "|" + itemType);
			if(null == itemsum){
				virCurrencyMap.put(itemId + "|" + itemType, currType + ":" + csum);
			}else{
				virCurrencyMap.put(itemId + "|" + itemType, itemsum + "," + currType + ":" + csum);
			}			
		}		
		
		for(String playerTp : playerTypeArr){			
			for (Map.Entry<String, String> entry : virCurrencyMap.entrySet()) {
				String keyMap = entry.getKey();
				String itemId = StringUtil.split(keyMap, "|")[0];
				String itemType = StringUtil.split(keyMap, "|")[1];
				String[] keyFields = new String[]{
						appID,
						platform,
						channel,
						gameServer,
						itemId,
						itemType,
						playerTp
				};
				mapValObj.set(entry.getValue());	//[currencyType:sum(itemCnt)|sum(virCurrency),]			
				mapKeyObj.setOutFields(keyFields);
				context.write(mapKeyObj, mapValObj);
			}
		}
		}catch(Throwable t){}
	}
}
