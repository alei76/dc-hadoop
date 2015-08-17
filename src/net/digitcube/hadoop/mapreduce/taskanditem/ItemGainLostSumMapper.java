package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.CommonExtend;
import net.digitcube.hadoop.mapreduce.domain.CommonHeader;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑
 * 对同一玩家当天的道具购买消耗情况进行汇总
 * 
 * 输入：APPID,Platform,GameServer,AccountID,ItemId,ItemType,channel,itemGetRecords,itemUseRecords
 * Map:
 * key：
 * 		APPID,Platform,Channel,GameServer,ItemId,ItemType
 * value：
 * 		[gainReason:sum(itemCnt)...],[lostReason:sum(itemCnt)...]
 * 
 * Reduce:
 * 		APPID,Platform,Channel,GameServer,ItemId,ItemType,type(道具产出消耗),vkey(),totalPlayer(总人数)
 * 		APPID,Platform,Channel,GameServer,ItemId,ItemType,type(道具产出),reason(产出方式),itemCnt(道具数量)
 * 		APPID,Platform,Channel,GameServer,ItemId,ItemType,type(道具消耗),reason(消耗方式),itemCnt(道具数量)
 */

public class ItemGainLostSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		//Added at 20140808 : 数据中存在异常数据导致字段换行等
		if(array.length != 10){
			return;
		}
		int i=0;
		String appID = array[i++];
		String platform = array[i++];
		String gameServer = array[i++];
		String accountId = array[i++];
		String playerType = array[i++];		
		String channel = array[i++];
		String itemGetRedords = array[i++];
		String itemUseRedords = array[i++];
		
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
		
		// 取出 itemId  -->  itemGetReason:sum
		Map<String, String> itemGetMap = new HashMap<String, String>();
		String[] itemGetArray = StringUtil.split(itemGetRedords,",");
		
		if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(itemGetRedords)){	
			for(String  getRecord: itemGetArray){	
				String[] getRecordArr = StringUtil.split(getRecord,":");
				String csum = getRecordArr[1];				
				String[] groupKey = StringUtil.split(getRecordArr[0], "|");
				String itemId = groupKey[0];
				String itemType = groupKey[1];
				String reason = groupKey[2];
				String  itemsum = itemGetMap.get(itemId + "|" + itemType);
				if(null == itemsum){
					itemGetMap.put(itemId + "|" + itemType, reason + ":" + csum);
				}else{
					itemGetMap.put(itemId + "|" + itemType, itemsum + "," + reason + ":" + csum);
				}			
			}		
		}
		
		// 取出 itemId  -->  itemUseReason:sum
		Map<String, String> itemUseMap = new HashMap<String, String>();
		String[] itemUseArray = StringUtil.split(itemUseRedords,",");
		
		if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(itemUseRedords)){
			for(String  useRecord: itemUseArray){		
				String[] useRecordArr = StringUtil.split(useRecord,":");
				String csum = useRecordArr[1];
				
				String[] groupKey = StringUtil.split(useRecordArr[0], "|");
				String itemId = groupKey[0];
				String itemType = groupKey[1];
				String reason = groupKey[2];
				String  itemsum = itemUseMap.get(itemId + "|" + itemType);
				if(null == itemsum){
					itemUseMap.put(itemId + "|" + itemType, reason + ":" + csum);
				}else{
					itemUseMap.put(itemId + "|" + itemType, itemsum + "," + reason + ":" + csum);
				}			
			}	
		}
		
		
		for(String playerTp : playerTypeArr){					
			// 获得
			for (Map.Entry<String, String> entry : itemGetMap.entrySet()) {
				String keyMap = entry.getKey();
				if(StringUtil.split(keyMap, "|").length < 2) continue;
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
				
				mapValObj.setSuffix("GET");
				mapValObj.setOutFields(new String[]{entry.getValue()});	//[getReason:sum(itemCnt),]			
				mapKeyObj.setOutFields(keyFields);
				context.write(mapKeyObj, mapValObj);
			}
			
			// 消耗
			for (Map.Entry<String, String> entry : itemUseMap.entrySet()) {
				String keyMap = entry.getKey();
				if(StringUtil.split(keyMap, "|").length < 2) continue;
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
				
				mapValObj.setSuffix("USE");
				mapValObj.setOutFields(new String[]{entry.getValue()});	//[useReason:sum(itemCnt),]			
				mapKeyObj.setOutFields(keyFields);
				context.write(mapKeyObj, mapValObj);
			}
		}	
			
	}
}
