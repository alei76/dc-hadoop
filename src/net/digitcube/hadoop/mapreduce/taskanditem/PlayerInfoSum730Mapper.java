package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.jce.UserExtendInfoLog.ExtendInfoDetail;
import net.digitcube.hadoop.jce.UserExtendInfoLog.ExtendInfoDetailMap;
import net.digitcube.hadoop.jce.UserExtendInfoLog.ItemDayInfo;
import net.digitcube.hadoop.model.UserExtInfoRollingLog;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PlayerInfoSum730Mapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
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
		
			String appID = array[0];
			String platform = array[1];			
			String gameServer = array[2];
			String accountId = array[3];
			String channel = array[4];
			String playerType7 = array[5];
			String playerType30 = array[6];
			String detailInfo = array[7];
			
			UserExtInfoRollingLog extInfoRollingLog = new UserExtInfoRollingLog( appID, platform,  channel, 
						gameServer,  accountId,  detailInfo);			
			ExtendInfoDetailMap eidMap = extInfoRollingLog.decodeFromBase64Str(detailInfo);
			// 道具购买7天数据			
			itemBuyNdaysOutPut(extInfoRollingLog,eidMap,context,7,statTime,"ITEMBUYCNT7","ITEMBUYVIRCUR7",playerType7);
			// 道具购买30天数据			
			itemBuyNdaysOutPut(extInfoRollingLog,eidMap,context,30,statTime,"ITEMBUYCNT30","ITEMBUYVIRCUR30",playerType30);
		
			
			 // 道具获得消耗7天数据   
			itemGetUseNdaysOutPut(extInfoRollingLog,context,7,statTime,"ITEMGET7","ITEMUSE7",playerType7);
			// 道具获得消耗30天数据   
			itemGetUseNdaysOutPut(extInfoRollingLog,context,30,statTime,"ITEMGET30","ITEMUSE30",playerType30);
			
	}
	
	
	// 道具购买n天数据
	private void itemBuyNdaysOutPut(UserExtInfoRollingLog extInfoRollingLog,ExtendInfoDetailMap eidMap,Context context,
				int days,int statTime,String suffixCnt,String suffixVirCur,String playerType)throws IOException,InterruptedException{
		List<Integer> daysList = DateUtil.getDateBeforeN(days,statTime);		
		Map<String, String> itemCntMap = new HashMap<String, String>();		
		Map<String, String> itemVirCurSumMap = new HashMap<String, String>();	
		
		String[] playerTypeArr = getPlayerType(playerType);
		if(null == playerTypeArr){
			return;
		}
		
		for(Integer day : daysList){
			ExtendInfoDetail eid = eidMap.getDetailMap().get(day);
			// 道具						
			if(null != eid){
				List<ItemDayInfo> itemList = eid.getItemDayInfoList();
				for(ItemDayInfo item : itemList){
					String itemId = item.getItemId();
					String itemType = item.getItemType();
					//购买道具数量
					Map<String,Integer> itemBuyCntMap = item.getBuyCntMap();
					for (Map.Entry<String, Integer> entry : itemBuyCntMap.entrySet()) {
						String currType = entry.getKey();
						Integer itemCnt = entry.getValue();
						String itemCurCnt = itemCntMap.get(itemId + "|" + itemType);
						if(null == itemCurCnt){
							itemCntMap.put(itemId + "|" + itemType ,currType + ":" + itemCnt);
						}else{
							itemCntMap.put(itemId + "|" + itemType ,itemCurCnt + "," + currType + ":" + itemCnt);
						}						
					}	
					
					//购买道具金额
					Map<String,Integer> itemBuyMap = item.getBuyMap();
					for (Map.Entry<String, Integer> entry : itemBuyMap.entrySet()) {
						String currType = entry.getKey();
						Integer itemVirCurSum = entry.getValue();
						String itemCurCnt = itemVirCurSumMap.get(itemId + "|" + itemType );
						if(null == itemCurCnt){
							itemVirCurSumMap.put(itemId + "|" + itemType, currType + ":" + itemVirCurSum);
						}else{
							itemVirCurSumMap.put(itemId + "|" + itemType ,itemCurCnt + "," + currType + ":" + itemVirCurSum);
						}						
					}		
				}
			}
		}	
		
		for(String playerTp : playerTypeArr){	
			//购买道具数量输出		
			for (Map.Entry<String, String> entry : itemCntMap.entrySet()) {
				String groupKey = entry.getKey();
				String[] keyArr = StringUtil.split(groupKey, "|");
				String itemCurCnt = entry.getValue() == null ? ItemGainLostForPlayerMapper.NO_RESULT: entry.getValue();					
				String[] keyFields = new String[]{
						extInfoRollingLog.getAppId(),extInfoRollingLog.getPlatform(),
						extInfoRollingLog.getChannel(),extInfoRollingLog.getGameServer(),
						keyArr[0],keyArr[1],playerTp
						} ;
				String[] valFields = new String[]{itemCurCnt};			
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				//SET SUFFIX
				mapValObj.setSuffix(suffixCnt);			
				context.write(mapKeyObj, mapValObj);				
			}	
			
			//购买道具金额输出		
			for (Map.Entry<String, String> entry : itemVirCurSumMap.entrySet()) {
				String groupKey = entry.getKey();
				String[] keyArr = StringUtil.split(groupKey, "|");
				String itemCurCnt = entry.getValue() == null ? ItemGainLostForPlayerMapper.NO_RESULT: entry.getValue();					
				String[] keyFields = new String[]{
						extInfoRollingLog.getAppId(),extInfoRollingLog.getPlatform(),
						extInfoRollingLog.getChannel(),extInfoRollingLog.getGameServer(),
						keyArr[0],keyArr[1],playerTp
						} ;
				String[] valFields = new String[]{itemCurCnt};			
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				//SET SUFFIX
				mapValObj.setSuffix(suffixVirCur);			
				context.write(mapKeyObj, mapValObj);				
			}				
		}
	}
	
	
	// 道具获得消耗n天数据
	private void itemGetUseNdaysOutPut(UserExtInfoRollingLog extInfoRollingLog,Context context,
			int days,int statTime,String suffixGet,String suffixUse,String playerType)throws IOException,	InterruptedException{
		List<Integer> daysList = DateUtil.getDateBeforeN(days,statTime);		
		Map<String, String> itemGetCntMap = new HashMap<String, String>();		
		Map<String, String> itemUseCntMap = new HashMap<String, String>();	
		
		String[] playerTypeArr = getPlayerType(playerType);
		if(null == playerTypeArr){
			return;
		}
		
		for(Integer day : daysList){
			// 道具			
			ExtendInfoDetail eid = extInfoRollingLog.getDetailMap().getDetailMap().get(day);
			if(null != eid){
				List<ItemDayInfo> itemList = eid.getItemDayInfoList();
				for(ItemDayInfo item : itemList){
					String itemId = item.getItemId();
					String itemType = item.getItemType();
					//获得道具数量
					Map<String,Integer> gainCntMap = item.getGainCntMap();
					for (Map.Entry<String, Integer> entry : gainCntMap.entrySet()) {
						String reason = entry.getKey();
						Integer itemCnt = entry.getValue();
						String reasonCount = itemGetCntMap.get(itemId + "|" + itemType );
						if(null == reasonCount){
							itemGetCntMap.put(itemId + "|" + itemType ,reason + ":" + itemCnt);
						}else{
							itemGetCntMap.put(itemId + "|" + itemType ,reasonCount + "," + reason + ":" + itemCnt);
						}						
					}	
					
					//消耗道具金额
					Map<String,Integer> useCntMap = item.getUseCntMap();
					for (Map.Entry<String, Integer> entry : useCntMap.entrySet()) {
						String reason = entry.getKey();
						Integer itemCnt = entry.getValue();
						String reasonCount = itemUseCntMap.get(itemId + "|" + itemType);
						if(null == reasonCount){
							itemUseCntMap.put(itemId + "|" + itemType ,reason + ":" + itemCnt);
						}else{
							itemUseCntMap.put(itemId + "|" + itemType ,reasonCount + "," + reason + ":" + itemCnt);
						}						
					}		
				}
			}
		}	
		
		for(String playerTp : playerTypeArr){	
			//获得道具数量
			for (Map.Entry<String, String> entry : itemGetCntMap.entrySet()) {
				String groupKey = entry.getKey();
				String[] keyArr = StringUtil.split(groupKey, "|");
				String itemGetCnt = entry.getValue() == null ? ItemGainLostForPlayerMapper.NO_RESULT: entry.getValue();
				String[] keyFields = new String[]{
							extInfoRollingLog.getAppId(),extInfoRollingLog.getPlatform(),
							extInfoRollingLog.getChannel(),extInfoRollingLog.getGameServer(),
							keyArr[0],keyArr[1],playerTp
						};
				String[] valFields = new String[]{itemGetCnt};	
				//SET SUFFIX
				mapValObj.setSuffix(suffixGet);
				mapKeyObj.setOutFields(keyFields);				
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);				
			}	
			
			// 消耗道具数量
			for (Map.Entry<String, String> entry : itemUseCntMap.entrySet()) {
				String groupKey = entry.getKey();
				String[] keyArr = StringUtil.split(groupKey, "|");
				String itemUseCnt = entry.getValue() == null ? ItemGainLostForPlayerMapper.NO_RESULT: entry.getValue();
				String[] keyFields = new String[]{
						extInfoRollingLog.getAppId(),extInfoRollingLog.getPlatform(),
						extInfoRollingLog.getChannel(),extInfoRollingLog.getGameServer(),
						keyArr[0],keyArr[1],playerTp};					
				String[] valFields = new String[]{itemUseCnt};	
				//SET SUFFIX
				mapValObj.setSuffix(suffixUse);
				mapKeyObj.setOutFields(keyFields);				
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);					
			}
		}
	}
	
	
	private String[] getPlayerType(String playerType){
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
		return playerTypeArr;	
	}

}
