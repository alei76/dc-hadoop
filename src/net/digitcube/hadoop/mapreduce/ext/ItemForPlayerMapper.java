package net.digitcube.hadoop.mapreduce.ext;

import java.io.IOException;
import java.util.Map;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import net.digitcube.hadoop.common.Constants;

/**
 * 
 * 主要逻辑 : 对同一玩家当天的道具购买、获得、消耗情况进行汇总
 * 输入：
 * a)道具购买-DESelf_ItemBuy
 * b)道具获得-DESelf_ItemGet
 * c)道具消耗-DESelf_ItemUse
 * 
 * 
 * Map: 
 * key： APPID,Platform,GameServer,AccountID,ItemId,ItemType 
 * value： channel,currencyType,itemCnt,vituralCurrency
 * 
 * Reduce: APPID,Platform,GameServer,AccountID,ItemId,ItemType
 * channel,[currencyType:sum(itemCnt)|sum(vituralCurrency)...]
 * 
 * 最终输出：
 * appId, appVersion, platform, channel, gameServer, accountId, uid, playerType
 * itemId, itemType, buyCount, getCount, useCount,
 * curTypeCntMap<curType:Cnt>
 * curTypeAmountMap<curType:Amount>
 * getReasonCntMap<reason:Cnt>
 * useReasonCntMap<reason:Cnt>
 */

public class ItemForPlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	public static final String DATA_FLAG_ITEM_BUY = "B";
	public static final String DATA_FLAG_ITEM_GET = "G";
	public static final String DATA_FLAG_ITEM_USE = "U";
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	
	// 当前输入的文件后缀
	private String fileSuffix = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 玩家上报的道具类型或道具名称中有换行符，需处理
		String line = value.toString().replaceAll("\n", "").replaceAll("\r", "");
		String[] array = line.split(MRConstants.SEPERATOR_IN);
		
		if (fileSuffix.endsWith(Constants.DESelf_ItemBuy)) {// 道具购买		
			EventLog eventLog = new EventLog(array);
			String appId = eventLog.getAppID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer = eventLog.getGameServer();
			String accountId = eventLog.getAccountID();
			String uid = eventLog.getUID();
			
			Map<String, String> map = eventLog.getArrtMap();
			String itemId = map.get("itemId");
			String itemType = map.get("itemType");
			if(StringUtil.isEmpty(itemType)){
				itemType = MRConstants.INVALID_PLACE_HOLDER_CHAR;
			}
			
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
			if(StringUtil.isEmpty(itemId) || StringUtil.isEmpty(itemType) || StringUtil.isEmpty(itemCnt) 
					|| StringUtil.isEmpty(currencyType) || StringUtil.isEmpty(virtualCurrency)){
				return;
			}
			
			String[] appIdAndVer = StringUtil.getAppIdAndVer(appId);
			String pureAppId = appIdAndVer[0];
			String appVersion = appIdAndVer[1];
			
			// 真实区服
			String[] keyFields = new String[] {
					pureAppId, 
					platform, 
					gameServer, 
					accountId, 
					itemId
			};
			String[] valFields = new String[] {
					appVersion,
					uid,
					channel,
					itemType,
					currencyType,
					itemCnt,
					virtualCurrency
			};
	
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			mapValObj.setSuffix(DATA_FLAG_ITEM_BUY);
			context.write(mapKeyObj, mapValObj);
	
			// 全服
			keyFields[2] = MRConstants.ALL_GAMESERVER;
			context.write(mapKeyObj, mapValObj);
			
		}else if(fileSuffix.endsWith(Constants.DESelf_ItemGet)
				|| fileSuffix.endsWith(Constants.DESelf_ItemUse)) { // 道具获得/消耗
			EventLog eventLog = new EventLog(array);
			String appId = eventLog.getAppID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer = eventLog.getGameServer();
			String accountId = eventLog.getAccountID();
			String uid = eventLog.getUID();
			
			Map<String, String> map = eventLog.getArrtMap();
			String itemId = map.get("itemId");
			String itemType = map.get("itemType");
			String itemCnt = map.get("itemCnt");
			String reason = map.get("reason");
			
			if(StringUtil.isEmpty(itemType)){
				itemType = MRConstants.INVALID_PLACE_HOLDER_CHAR;
			}
			if(StringUtil.isEmpty(itemCnt)){
				itemType = MRConstants.INVALID_PLACE_HOLDER_NUM;
			}
			if(StringUtil.isEmpty(reason)){
				itemType = MRConstants.INVALID_PLACE_HOLDER_CHAR;
			}
			
			//Added at 20140710: 与高境约定异常数据暂时丢弃
			if(StringUtil.isEmpty(itemId) || StringUtil.isEmpty(itemType) || StringUtil.isEmpty(itemCnt) || StringUtil.isEmpty(reason)){
				return;
			}
			
			String[] appIdAndVer = StringUtil.getAppIdAndVer(appId);
			String pureAppId = appIdAndVer[0];
			String appVersion = appIdAndVer[1];
			
			//真实区服
			String[] keyFields = new String[]{
					pureAppId,
					platform,
					gameServer,
					accountId,
					itemId
			};
			String[] valFields = new String[]{
					appVersion,
					uid,
					channel,
					itemType,
					reason,
					itemCnt
			};				
			
			String suffix = "";
			if(fileSuffix.endsWith(Constants.DESelf_ItemGet)){
				suffix = DATA_FLAG_ITEM_GET;
			}else if(fileSuffix.endsWith(Constants.DESelf_ItemUse)){
				suffix = DATA_FLAG_ITEM_USE;
			}
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			mapValObj.setSuffix(suffix);
			context.write(mapKeyObj, mapValObj);
			
			//全服
			keyFields[2] = MRConstants.ALL_GAMESERVER;
			context.write(mapKeyObj, mapValObj);
			
		}
		/*else if (fileSuffix.endsWith(Constants.DESelf_ItemUse)) { // 道具消耗日志
			EventLog eventLog = new EventLog(array);
			String appId = eventLog.getAppID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer = eventLog.getGameServer();
			String accountId = eventLog.getAccountID();
			
			Map<String, String> map = eventLog.getArrtMap();
			String itemId = map.get("itemId");
			String itemType = map.get("itemType");
			if(StringUtil.isEmpty(itemType)){
				itemType = MRConstants.INVALID_PLACE_HOLDER_CHAR;
			}
			
			String itemCnt = map.get("itemCnt");
			String reason = map.get("reason");
			
			//Added at 20140710: 与高境约定异常数据暂时丢弃
			if(StringUtil.isEmpty(itemId) || StringUtil.isEmpty(itemType) || StringUtil.isEmpty(itemCnt) || StringUtil.isEmpty(reason)){
				return;
			}
			
			//真实区服
			String[] keyFields = new String[]{
					appId,
					platform,
					gameServer,
					accountId,
					itemId,
					itemType
			};
			String[] valFields = new String[]{
					channel,
					reason,
					itemCnt
			};				
			
			mapKeyObj.setSuffix(DATA_FLAG_ITEM_USE);
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			context.write(mapKeyObj, mapValObj);
			
			//全服
			keyFields[2] = MRConstants.ALL_GAMESERVER;
			context.write(mapKeyObj, mapValObj);
		}*/
	}
}
