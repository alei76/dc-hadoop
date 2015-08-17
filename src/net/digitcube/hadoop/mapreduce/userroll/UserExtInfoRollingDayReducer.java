package net.digitcube.hadoop.mapreduce.userroll;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.jce.UserExtendInfoLog.ExtendInfoDetail;
import net.digitcube.hadoop.jce.UserExtendInfoLog.ItemDayInfo;
import net.digitcube.hadoop.jce.UserExtendInfoLog.LevelDayInfo;
import net.digitcube.hadoop.jce.UserExtendInfoLog.TaskDayInfo;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemGainLostForPlayerMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.TaskStatMapper;
import net.digitcube.hadoop.model.UserExtInfoRollingLog;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class UserExtInfoRollingDayReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable>{

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	
	private Map<String, ItemDayInfo> itemDayMap = new HashMap<String, ItemDayInfo>();
	private Map<String, LevelDayInfo> levelDayMap = new HashMap<String, LevelDayInfo>();
	private Map<String, TaskDayInfo> taskDayMap = new HashMap<String, TaskDayInfo>();
	
	private int statTime;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statTime = DateUtil.getStatDateForHourOrToday(context.getConfiguration());
	}


	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		try{
		
		itemDayMap.clear();
		levelDayMap.clear();
		taskDayMap.clear();
		
		UserExtInfoRollingLog extInfoRollingLog = null;
		String maxVersion = "";
		String channel = null;
		String firstLoginDate = null;
		String totalPayCur = null;
		for (OutFieldsBaseModel val : values) {
			int i = 0;
			String[] fields = val.getOutFields();
			String dataFlag = fields[i++];
			String tmpAppVer = fields[i++];
			channel = fields[i++];
			if(tmpAppVer.compareTo(maxVersion) > 0){
				maxVersion = tmpAppVer; 
			}
			if(UserExtInfoRollingDayMap.DATA_FLAG_DETAIL_ROLLING.equals(dataFlag)){
				String appId = key.getOutFields()[0];
				String platform = key.getOutFields()[1];
				String gameServer = key.getOutFields()[2];
				String accountId = key.getOutFields()[3];
				
				String detailInfo = fields[i];
				extInfoRollingLog = new UserExtInfoRollingLog(appId, platform, channel, gameServer, accountId, detailInfo);
			}else if(UserExtInfoRollingDayMap.DATA_FLAG_ITEM_BUY.equals(dataFlag)){
				String playerType = fields[i++];
				firstLoginDate = fields[i++];
				totalPayCur = fields[i++];
				String buyRecords = fields[i++];// [itemId|itemType|currencyType:sum(itemCnt)|sum(virCurrency),]				
				// 取出 itemId  -->  curType:sum
				Map<String, String> virCurrencyMap = new HashMap<String, String>();
				String[] groupArray = StringUtil.split(buyRecords,",");					
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
				
				// 对itemId封装  并将购买币种对应 金额记录进滚存  
				for (Map.Entry<String, String> entry : virCurrencyMap.entrySet()) {
					String keyMap = entry.getKey();
					String itemId = StringUtil.split(keyMap, "|")[0];
					String itemType = StringUtil.split(keyMap, "|")[1];
					String itemRecords = entry.getValue();//[currencyType:sum(itemCnt)|sum(virCurrency),]	
					//记录玩家 货币类型和数量  
					String[] currTypeAndVals = StringUtil.split(itemRecords, ",");
					//把各种 currencyType 购买过该道具的数量累加起来
					int totalBuyCount = 0;	
					ItemDayInfo itemInfo = itemDayMap.get(itemId);
					if(null == itemInfo){
						itemInfo = new ItemDayInfo();
						itemInfo.setItemId(itemId);
						itemInfo.setItemType(itemType);
						itemDayMap.put(itemId, itemInfo);
					}else{
						totalBuyCount = itemInfo.getBuyCount();
					}		
					
					Map<String, Integer> buyMap = null;
					Map<String, Integer> buyCntMap = null;
					buyMap = itemInfo.getBuyMap();
					buyCntMap = itemInfo.getBuyCntMap();
					if(null == buyMap){
						buyMap = new HashMap<String, Integer>();
					}
					if(null == buyCntMap){
						buyCntMap = new HashMap<String, Integer>();
					}
					// 统计道具购买    币种对用 金额map
					for(String typeAndVal : currTypeAndVals){
						String[] currTypeAndVal = StringUtil.split(typeAndVal, ":");
						String currencyType = currTypeAndVal[0];
						String[] vals = StringUtil.split(currTypeAndVal[1],"|");
						int itemCnt = StringUtil.convertInt(vals[0], 0);
						int virCurrency = StringUtil.convertInt(vals[1], 0);				
						//记录道具购买数量 
						totalBuyCount += itemCnt;
						//记录道具购买    币种对用 道具数量 map
						Integer itemCntTotal =  buyCntMap.get(currencyType);
						if(null == itemCntTotal){
							buyCntMap.put(currencyType, itemCnt);
						}else{
							itemCntTotal =  buyCntMap.get(currencyType);
							buyCntMap.put(currencyType, itemCntTotal + itemCnt);
						}			
						
						//记录道具购买    币种对用 金额map
						Integer virCurrencyTotal =  buyMap.get(currencyType);
						if(null == virCurrencyTotal){
							buyMap.put(currencyType, virCurrency);
						}else{
							virCurrencyTotal =  buyMap.get(currencyType);
							buyMap.put(currencyType, virCurrencyTotal + virCurrency);
						}					
					}			
					itemInfo.setBuyCount(totalBuyCount);
					itemInfo.setBuyMap(buyMap);		
					itemInfo.setBuyCntMap(buyCntMap);	
					itemDayMap.put(itemId, itemInfo);
					
				}		
				
			}else if(UserExtInfoRollingDayMap.DATA_FLAG_ITEM_GET_USE.equals(dataFlag)){
				String playerType = fields[i++];
				firstLoginDate = fields[i++];
				totalPayCur = fields[i++];
				String itemGetRedords = fields[i++];// [itemId|itemType|getReason:sum(itemCnt),...]
				String itemUseRedords = fields[i++];// [itemId|itemType|useReason:sum(itemCnt),...]
				
				
				// 取出 itemId  -->  getreason:sum
				Map<String, String> getReasonMap = new HashMap<String, String>();
				String[] getGroupArray = StringUtil.split(itemGetRedords,",");	
				
				if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(itemGetRedords)){	
					for(String getGroupReasonSum : getGroupArray){		
						String[] reasonSum = StringUtil.split(getGroupReasonSum,":");
						String csum = reasonSum[1];
						
						String[] groupKey = StringUtil.split(reasonSum[0], "|");
						String itemId = groupKey[0];
						String itemType = groupKey[1];
						String reason = groupKey[2];
						String  getReasonSum = getReasonMap.get(itemId + "|" + itemType);
						if(null == getReasonSum){
							getReasonMap.put(itemId + "|" + itemType, reason + ":" + csum);
						}else{
							getReasonMap.put(itemId + "|" + itemType, getReasonSum + "," + reason + ":" + csum);
						}			
					}
				}
				
				// 取出 itemId  -->  usereason:sum
				Map<String, String> useReasonMap = new HashMap<String, String>();
				String[] useGroupArray = StringUtil.split(itemUseRedords,",");	
				if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(itemUseRedords)){
					for(String useGroupReasonSum : useGroupArray){		
						String[] reasonSum = StringUtil.split(useGroupReasonSum,":");
						if(reasonSum.length < 2){
							continue;
						}
						String csum = reasonSum[1];
						
						String[] groupKey = StringUtil.split(reasonSum[0], "|");
						String itemId = groupKey[0];
						String itemType = groupKey[1];
						String reason = groupKey[2];
						String  useReasonSum = useReasonMap.get(itemId + "|" + itemType);
						if(null == useReasonSum){
							useReasonMap.put(itemId + "|" + itemType, reason + ":" + csum);
						}else{
							useReasonMap.put(itemId + "|" + itemType, useReasonSum + "," + reason + ":" + csum);
						}			
					}	
				}
				
				// 对itemId封装  并将获得方式对应 获得记录进滚存  
				for (Map.Entry<String, String> entry : getReasonMap.entrySet()) {
					String keyMap = entry.getKey();
					if(StringUtil.split(keyMap, "|").length < 2){
						continue;
					} 
					String itemId = StringUtil.split(keyMap, "|")[0];
					String itemType = StringUtil.split(keyMap, "|")[1];
					String itemRecords = entry.getValue();//[reason:sum(itemCnt),]	
					//记录玩家 获得方式和数量  
					String[] reasonSumArr = StringUtil.split(itemRecords, ",");
					//把各种 reason 的数量累加起来
					int totalGetCount = 0;	
					ItemDayInfo itemInfo = itemDayMap.get(itemId);
					if(null == itemInfo){
						itemInfo = new ItemDayInfo();
						itemInfo.setItemId(itemId);
						itemInfo.setItemType(itemType);
						itemDayMap.put(itemId, itemInfo);
					}else{
						totalGetCount = itemInfo.getGainCount();
					}		
					
					Map<String, Integer> getCntMap = null;				
					getCntMap = itemInfo.getGainCntMap();
					if(null == getCntMap){
						getCntMap = new HashMap<String, Integer>();
					}
		
					// 统计道具获得方式对应数量map
					for(String  reasonSum: reasonSumArr){
						String[] rsArr = StringUtil.split(reasonSum, ":");
						if(rsArr.length < 2){
							continue;
						}
						String reason = rsArr[0];						
						int sum = StringUtil.convertInt(rsArr[1], 0);					
						//记录道具获得数量 
						totalGetCount += sum;
						//记录道具购买    方式对用 道具数量 map
						Integer rsTotal =  getCntMap.get(reason);
						if(null == rsTotal){
							getCntMap.put(reason, sum);
						}else{
							rsTotal =  getCntMap.get(reason);
							getCntMap.put(reason, rsTotal + sum);
						}								
					}			
					itemInfo.setGainCount(totalGetCount);		
					itemInfo.setGainCntMap(getCntMap);		
					itemDayMap.put(itemId, itemInfo);
				}						
				
				// 对itemId封装  并将消耗方式对应 获得记录进滚存  
				for (Map.Entry<String, String> entry : useReasonMap.entrySet()) {
					String keyMap = entry.getKey();
					if(StringUtil.split(keyMap, "|").length < 2) {
						continue;
					}
					String itemId = StringUtil.split(keyMap, "|")[0];
					String itemType = StringUtil.split(keyMap, "|")[1];
					String itemRecords = entry.getValue();//[reason:sum(itemCnt),]	
					//记录玩家 获得方式和数量  
					String[] reasonSumArr = StringUtil.split(itemRecords, ",");
					//把各种 reason 的数量累加起来
					int totalUseCount = 0;	
					ItemDayInfo itemInfo = itemDayMap.get(itemId);
					if(null == itemInfo){
						itemInfo = new ItemDayInfo();
						itemInfo.setItemId(itemId);
						itemInfo.setItemType(itemType);
						itemDayMap.put(itemId, itemInfo);
					}else{
						totalUseCount = itemInfo.getUseCount();
					}		
					
					Map<String, Integer> useCntMap = null;				
					useCntMap = itemInfo.getUseCntMap();
					if(null == useCntMap){
						useCntMap = new HashMap<String, Integer>();
					}
		
					// 统计道具消耗方式对应数量map
					for(String  reasonSum: reasonSumArr){
						String[] rsArr = StringUtil.split(reasonSum, ":");
						if(rsArr.length < 2){
							continue;
						}
						String reason = rsArr[0];						
						int sum = StringUtil.convertInt(rsArr[1], 0);					
						//记录道具消耗数量 
						totalUseCount += sum;
						//记录道具消耗    方式对用 道具数量 map
						Integer rsTotal =  useCntMap.get(reason);
						if(null == rsTotal){
							useCntMap.put(reason, sum);
						}else{
							rsTotal =  useCntMap.get(reason);
							useCntMap.put(reason, rsTotal + sum);
						}								
					}			
					itemInfo.setUseCount(totalUseCount);			
					itemInfo.setUseCntMap(useCntMap);	
					itemDayMap.put(itemId, itemInfo);
				}		
								
			}else if(UserExtInfoRollingDayMap.DATA_FLAG_GUANKA.equals(dataFlag)){
				String levelId = fields[i++];
				String seqno = fields[i++];
				String tmpDataFlag = fields[i++];
				int times = StringUtil.convertInt(fields[i++],0);
				
				LevelDayInfo levelInfo = levelDayMap.get(levelId);
				if(null == levelInfo){
					levelInfo = new LevelDayInfo(); 
					levelInfo.setLevelId(levelId);
					levelInfo.setSeqno(seqno);
					
					levelDayMap.put(levelId, levelInfo);
				}
				
				if(Constants.DATA_FLAG_GUANKA_BEGIN_TIMES.equals(tmpDataFlag)){
					levelInfo.setBeginTimes(levelInfo.getBeginTimes() + times);
					
				}else if(Constants.DATA_FLAG_GUANKA_SUCCESS_TIMES.equals(tmpDataFlag)){
					levelInfo.setSuccessTimes(levelInfo.getSuccessTimes() + times);
					
				}else if(Constants.DATA_FLAG_GUANKA_FAILED_TIMES.equals(tmpDataFlag)){
					levelInfo.setFailueTimes(levelInfo.getFailueTimes() + times);
				}
				
			}else if(UserExtInfoRollingDayMap.DATA_FLAG_TASK.equals(dataFlag)){
				
				String taskId = fields[i++];
				String taskType = fields[i++];
				String tmpDataFlag = fields[i++];
				
				TaskDayInfo taskInfo = taskDayMap.get(taskId);
				if(null == taskInfo){
					taskInfo = new TaskDayInfo(); 
					taskInfo.setTaskId(taskId);
					taskInfo.setTaskType(taskType);
					taskDayMap.put(taskId, taskInfo);
				}
				
				if(TaskStatMapper.TASK_BEGIN.equals(tmpDataFlag)){
					taskInfo.setBeginTimes(taskInfo.getBeginTimes() + 1);
					
				}else if(TaskStatMapper.TASK_SUCCESS.equals(tmpDataFlag)){
					taskInfo.setSuccessTimes(taskInfo.getSuccessTimes() + 1);
					
				}else if(TaskStatMapper.TASK_FAILED.equals(tmpDataFlag)){
					taskInfo.setFailueTimes(taskInfo.getFailueTimes() + 1);
				}
			}
		}
		
		String appidAndVersion = key.getOutFields()[0] + "|" + maxVersion; 
		if(null == extInfoRollingLog){
			extInfoRollingLog = new UserExtInfoRollingLog();
			extInfoRollingLog.setAppId(appidAndVersion); //设置最大版本
			extInfoRollingLog.setPlatform(key.getOutFields()[1]);
			extInfoRollingLog.setChannel(channel);
			extInfoRollingLog.setGameServer(key.getOutFields()[2]);
			extInfoRollingLog.setAccountID(key.getOutFields()[3]);
		}else{
			//设置最大版本
			extInfoRollingLog.setAppId(appidAndVersion);
		}		
		
		if(itemDayMap.size() > 0 || levelDayMap.size() > 0 || taskDayMap.size() > 0){
			ExtendInfoDetail extInfoDetail = new ExtendInfoDetail();
			//道具
			ArrayList<ItemDayInfo> itemDayList = new ArrayList<ItemDayInfo>(itemDayMap.values());		
			extInfoDetail.setItemDayInfoList(itemDayList);
			//关卡
			ArrayList<LevelDayInfo> levelDayList = new ArrayList<LevelDayInfo>(levelDayMap.values());
			extInfoDetail.setLevelDayInfoList(levelDayList);
			//任务
			ArrayList<TaskDayInfo> taskDayList = new ArrayList<TaskDayInfo>(taskDayMap.values());
			extInfoDetail.setTaskDayInfoList(taskDayList);
					
			extInfoRollingLog.getDetailMap().getDetailMap().put(statTime, extInfoDetail);		
		}
		
		//20141218：清理数据
		//a, 当天没有数据的话则把该元素从 map 中移除（这是之前的bug，不管是否有数据都放到map里）
		//b, 不再保存道具中各个币种的购买详情，数据量太大
		//c, 下周回滚到老的滚存方式
		Map<Integer, ExtendInfoDetail> detailMap = extInfoRollingLog.getDetailMap().getDetailMap();
		Set<Integer> toRemoveSet = new HashSet<Integer>();
		for(Entry<Integer, ExtendInfoDetail> entry : detailMap.entrySet()){
			ExtendInfoDetail detail = entry.getValue();
			if((null == detail.itemDayInfoList || detail.itemDayInfoList.isEmpty())
					|| (null == detail.levelDayInfoList || detail.levelDayInfoList.isEmpty())
					|| (null == detail.taskDayInfoList || detail.taskDayInfoList.isEmpty())){
				//之前的 bug 导致有空数据，删除掉
				toRemoveSet.add(entry.getKey());
				continue;
			}
			
			//不是空数据，把道具中按币种购买的详情删掉
			if(null != detail.itemDayInfoList){
				for(ItemDayInfo info : detail.itemDayInfoList){
					if(null != info.buyMap){
						info.buyMap.clear();
					}
					if(null != info.buyCntMap){
						info.buyCntMap.clear();
					}
					if(null != info.gainCntMap){
						info.gainCntMap.clear();
					}
					if(null != info.useCntMap){
						info.useCntMap.clear();
					}
				}
			}
		}
		for(Integer toRemove : toRemoveSet){
			detailMap.remove(toRemove);
		}
		
		
		//输出
		keyObj.setOutFields(extInfoRollingLog.toStringArray());
		keyObj.setSuffix(Constants.SUFFIX_EXT_INFO_ROLL_DAY);
		
		context.write(keyObj, NullWritable.get());
		
	}catch(Throwable t){}
	}	
}
