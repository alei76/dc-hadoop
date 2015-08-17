package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.Map;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import net.digitcube.hadoop.common.Constants;

/**
 * 
 * 主要逻辑 : 对同一玩家当天的道具购买消耗情况进行汇总 道具购买日志
 * 
 * Map: key： APPID,Platform,GameServer,AccountID,ItemId,ItemType value： channel,currencyType,itemCnt,vituralCurrency
 * 
 * Reduce: APPID,Platform,GameServer,AccountID,ItemId,ItemType
 * channel,[currencyType:sum(itemCnt)|sum(vituralCurrency)...]
 */

public class ItemBuyForPlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	
	// 当前输入的文件后缀
	private String fileSuffix = "";
	// 统计的数据时间
	private int statTime = 0;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		statTime = DateUtil.getStatDateForHourOrToday(context.getConfiguration());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		
		if (fileSuffix.contains("DESelf_ItemBuy")) { // 道具购买事件日志		
			EventLog eventLog = new EventLog(array);
			String appID = eventLog.getAppID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer = eventLog.getGameServer();
			String accountId = eventLog.getAccountID();
	
			Map<String, String> map = eventLog.getArrtMap();
			String itemId = map.get("itemId");
			String itemType = map.get("itemType");
			String currencyType = map.get("currencyType");
			String itemCnt = map.get("itemCnt");
			String virtualCurrency = map.get("virtualCurrency");
			if (null == virtualCurrency) {
				virtualCurrency = map.get("vituralCurrency");
			}
	
			if (StringUtil.isEmpty(itemType)) {
				itemType = MRConstants.INVALID_PLACE_HOLDER_CHAR;
			}
			if(StringUtil.isEmpty(currencyType)){
				currencyType = MRConstants.INVALID_PLACE_HOLDER_CHAR;
			}
			if(StringUtil.isEmpty(itemCnt)){
				itemCnt = MRConstants.INVALID_PLACE_HOLDER_NUM;
			}
			if(StringUtil.isEmpty(virtualCurrency)){
				itemCnt = MRConstants.INVALID_PLACE_HOLDER_NUM;
			}
			//Added at 20140703: 与高境约定异常数据暂时丢弃
			/*if(null == itemId || null == itemType || null == currencyType || null == itemCnt || null == virtualCurrency){
				return;
			}*/
			if(StringUtil.isEmpty(itemId) || StringUtil.isEmpty(itemType) || StringUtil.isEmpty(itemCnt) 
					|| StringUtil.isEmpty(currencyType) || StringUtil.isEmpty(virtualCurrency)){
				return;
			}
			
			// 真实区服
			String[] keyFields = new String[] { appID, platform, gameServer, accountId };
			String[] valFields = new String[] { channel,itemId, itemType, currencyType, itemCnt, virtualCurrency };
	
			mapValObj.setSuffix("ITEMBUY");
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			context.write(mapKeyObj, mapValObj);
	
			// 全服
			keyFields[2] = MRConstants.ALL_GAMESERVER;
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			context.write(mapKeyObj, mapValObj);		
		}else if(fileSuffix.contains(Constants.SUFFIX_PLAYER_NEWADD_AND_PAY)){	// 滚存日志			
			String appID = array[0];
			String platform = array[1];
			String channel = array[2];
			String gameServer = array[3];
			String accountId = array[4];
			String firstLoginDate = array[5];
			String totalPayCur = array[6];
			String payTrack = array[7];
			String[] keyFields = new String[] { appID, platform, gameServer, accountId };
			String[] valFields = new String[] { firstLoginDate, totalPayCur};
			mapValObj.setSuffix("PLAYERNEWPAY");
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			context.write(mapKeyObj, mapValObj);			
		}
	}
}
