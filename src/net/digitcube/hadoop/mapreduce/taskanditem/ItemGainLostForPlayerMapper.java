package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑
 * 对同一玩家当天道具的获得与消耗情况进行汇总
 * 
 * Map:
 * key：
 * 		APPID,Platform,GameServer,AccountID,ItemId,ItemType
 * value：
 * 		channel,GetOrUse,reason,itemCnt
 * 
 * Reduce:
 * 		APPID,Platform,GameServer,AccountID,ItemId,ItemType,channel,[getReason:sum(itemCnt),...],[useReason:sum(itemCnt),...]
 * 
 * 当获得方式或消耗方式其中一个不存在是，则用  NO_RESULT 占位符代替
 */

public class ItemGainLostForPlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	static final String ITEM_GET = "G";
	static final String ITEM_USE = "U";
	//20140710: 与境哥、sandy 确认，道具购买也认为是道具产出的一种
	//201407某日: 与境哥、sandy 重新确认，否定之前说法，现在认为道具购买不算是道具产出的一种
	static final String ITEM_BUY = "B";
	//道具购买也认为是产出的一种，产出方式用 USER_BUY 表示
	//static final String REASON_USER_BUY = "USER_BUY";
	//当获得方式或消耗方式其中一个不存在是，则用  NO_RESULT 占位符代替
	public static final String NO_RESULT = "NO_RESULT";
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	private String GetOrUse = ""; // 道具获得或消耗标志
	
	// 当前输入的文件后缀
	private String fileSuffix = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		if(fileSuffix.contains(Constants.DESelf_ItemGet)){
			GetOrUse = ITEM_GET;
		}else if(fileSuffix.contains(Constants.DESelf_ItemUse)){
			GetOrUse = ITEM_USE;
		}else if(fileSuffix.contains(Constants.DESelf_ItemBuy)){
			GetOrUse = ITEM_BUY;
		}
	}


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//201407某日: 与境哥、sandy 重新确认，不再认为道具购买是道具产出的一种
		if(ITEM_BUY.equals(GetOrUse)){
			return;
		}
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		if (fileSuffix.contains(Constants.DESelf_ItemGet) 
				|| fileSuffix.contains(Constants.DESelf_ItemUse)) { // 道具获得和消耗				
				EventLog eventLog = new EventLog(array);
				String appID = eventLog.getAppID();
				String platform = eventLog.getPlatform();
				String channel = eventLog.getChannel();
				String gameServer = eventLog.getGameServer();
				String accountId = eventLog.getAccountID();
				
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
				//20140710:道具购买也认为是产出的一种，产出方式用 USER_BUY 表示
				/*if(ITEM_BUY.equals(GetOrUse)){
					reason = REASON_USER_BUY; 
				}*/
				
				//Added at 20140710: 与高境约定异常数据暂时丢弃
				/*if(null == itemId || null == itemType || null == itemCnt || null == reason){
					return;
				}*/
				if(StringUtil.isEmpty(itemId) || StringUtil.isEmpty(itemType) || StringUtil.isEmpty(itemCnt) || StringUtil.isEmpty(reason)){
					return;
				}
				
				//真实区服
				String[] keyFields = new String[]{
						appID,
						platform,
						gameServer,
						accountId						
				};
				String[] valFields = new String[]{
						channel,
						itemId,
						itemType,
						GetOrUse,
						reason,
						itemCnt
				};				
				
				mapValObj.setSuffix("GETUSE");
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
				
				//全服
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
