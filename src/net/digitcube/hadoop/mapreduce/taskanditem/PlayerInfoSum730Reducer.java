package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class PlayerInfoSum730Reducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	private Map<String, Integer> itemBuyCnt7Map = new HashMap<String, Integer>();
	private Map<String, Integer> itemBuyCnt30Map = new HashMap<String, Integer>();
	private Map<String, Integer> itemBuyVirCnt7Map = new HashMap<String, Integer>();
	private Map<String, Integer> itemBuyVirCnt30Map = new HashMap<String, Integer>();
	
	private Map<String, Integer> itemBuyPlayerNum7Map = new HashMap<String, Integer>();
	private Map<String, Integer> itemBuyPlayerNum30Map = new HashMap<String, Integer>();
	
	private Map<String, Integer> itemGetCnt7Map = new HashMap<String, Integer>();
	private Map<String, Integer> itemGetCnt30Map = new HashMap<String, Integer>();
	private Map<String, Integer> itemUseCnt7Map = new HashMap<String, Integer>();
	private Map<String, Integer> itemUseCnt30Map = new HashMap<String, Integer>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		itemBuyCnt7Map.clear();
		itemBuyCnt30Map.clear();
		itemBuyVirCnt7Map.clear();
		itemBuyVirCnt30Map.clear();
		
		itemBuyPlayerNum7Map.clear();
		itemBuyPlayerNum30Map.clear();
		
		itemGetCnt7Map.clear();
		itemGetCnt30Map.clear();
		itemUseCnt7Map.clear();
		itemUseCnt30Map.clear();
		
		int totalPlayer7 = 0;
		int totalPlayer30 = 0;		
		
		for(OutFieldsBaseModel val : values){			
			if("ITEMBUYCNT7".equals(val.getSuffix())){
				String[] valFields =  val.getOutFields();				
				if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(valFields[0])){
					String[] currTypeAndVals = StringUtil.split(valFields[0], ",");
					for(String typeAndVal : currTypeAndVals){
						String[] currTypeAndVal = StringUtil.split(typeAndVal,":");
						String currencyType = currTypeAndVal[0];
						String vals = currTypeAndVal[1];
						int itemCnt = StringUtil.convertInt(vals, 0);					
						
						Integer count = itemBuyCnt7Map.get(currencyType);
						if(null == count){
							itemBuyCnt7Map.put(currencyType, itemCnt);
						}else{
							itemBuyCnt7Map.put(currencyType, itemCnt + count);
						}		
						
						// 币种人数统计
						Integer num = itemBuyPlayerNum7Map.get(currencyType);
						if(null == num){
							itemBuyPlayerNum7Map.put(currencyType, 1);
						}else{
							itemBuyPlayerNum7Map.put(currencyType, num + 1);
						}
					}
				}
			}else if("ITEMBUYCNT30".equals(val.getSuffix())){
				String[] valFields =  val.getOutFields();
				if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(valFields[0])){
					String[] currTypeAndVals = StringUtil.split(valFields[0], ",");
					for(String typeAndVal : currTypeAndVals){
						String[] currTypeAndVal = StringUtil.split(typeAndVal,":");
						String currencyType = currTypeAndVal[0];
						String vals = currTypeAndVal[1];
						int itemCnt = StringUtil.convertInt(vals, 0);	
						
						Integer count = itemBuyCnt30Map.get(currencyType);
						if(null == count){
							itemBuyCnt30Map.put(currencyType, itemCnt);
						}else{
							itemBuyCnt30Map.put(currencyType, itemCnt + count);
						}		
						
						// 币种人数统计
						Integer num = itemBuyPlayerNum30Map.get(currencyType);
						if(null == num){
							itemBuyPlayerNum30Map.put(currencyType, 1);
						}else{
							itemBuyPlayerNum30Map.put(currencyType, num + 1);
						}
					}
				}
			}else if("ITEMBUYVIRCUR7".equals(val.getSuffix())){
				String[] valFields =  val.getOutFields();
				if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(valFields[0])){
					String[] currTypeAndVals = StringUtil.split(valFields[0], ",");
					for(String typeAndVal : currTypeAndVals){
						String[] currTypeAndVal = StringUtil.split(typeAndVal,":");
						String currencyType = currTypeAndVal[0];
						String vals = currTypeAndVal[1];
						int itemCnt = StringUtil.convertInt(vals, 0);	
						
						Integer count = itemBuyVirCnt7Map.get(currencyType);
						if(null == count){
							itemBuyVirCnt7Map.put(currencyType, itemCnt);
						}else{
							itemBuyVirCnt7Map.put(currencyType, itemCnt + count);
						}				
					}
				}
			}else if("ITEMBUYVIRCUR30".equals(val.getSuffix())){
				String[] valFields =  val.getOutFields();
				if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(valFields[0])){
					String[] currTypeAndVals = StringUtil.split(valFields[0], ",");
					for(String typeAndVal : currTypeAndVals){
						String[] currTypeAndVal = StringUtil.split(typeAndVal,":");
						String currencyType = currTypeAndVal[0];
						String vals = currTypeAndVal[1];
						int itemCnt = StringUtil.convertInt(vals, 0);	
						
						Integer count = itemBuyVirCnt30Map.get(currencyType);
						if(null == count){
							itemBuyVirCnt30Map.put(currencyType, itemCnt);
						}else{
							itemBuyVirCnt30Map.put(currencyType, itemCnt + count);
						}				
					}
				}
			}else if("ITEMGET7".equals(val.getSuffix())){
				String[] valFields =  val.getOutFields();
				if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(valFields[0])){
					totalPlayer7++ ;
					String[] reasonTypeAndVals = StringUtil.split(valFields[0], ",");
					for(String typeAndVal : reasonTypeAndVals){
						String[] reasonTypeAndVal = StringUtil.split(typeAndVal,":");
						String reasonType = reasonTypeAndVal[0];
						String vals = reasonTypeAndVal[1];
						int itemCnt = StringUtil.convertInt(vals, 0);	
						
						Integer count = itemGetCnt7Map.get(reasonType);
						if(null == count){
							itemGetCnt7Map.put(reasonType, itemCnt);
						}else{
							itemGetCnt7Map.put(reasonType, itemCnt + count);
						}				
					}
				}
			}else if("ITEMGET30".equals(val.getSuffix())){
				String[] valFields =  val.getOutFields();
				if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(valFields[0])){
					totalPlayer30++;
					String[] reasonTypeAndVals = StringUtil.split(valFields[0], ",");
					for(String typeAndVal : reasonTypeAndVals){
						String[] reasonTypeAndVal = StringUtil.split(typeAndVal,":");
						String reasonType = reasonTypeAndVal[0];
						String vals = reasonTypeAndVal[1];
						int itemCnt = StringUtil.convertInt(vals, 0);	
						
						Integer count = itemGetCnt30Map.get(reasonType);
						if(null == count){
							itemGetCnt30Map.put(reasonType, itemCnt);
						}else{
							itemGetCnt30Map.put(reasonType, itemCnt + count);
						}				
					}
				}
			}else if("ITEMUSE7".equals(val.getSuffix())){
				String[] valFields =  val.getOutFields();
				if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(valFields[0])){
					String[] reasonTypeAndVals = StringUtil.split(valFields[0], ",");
					for(String typeAndVal : reasonTypeAndVals){
						String[] reasonTypeAndVal = StringUtil.split(typeAndVal,":");
						String reasonType = reasonTypeAndVal[0];
						String vals = reasonTypeAndVal[1];
						int itemCnt = StringUtil.convertInt(vals, 0);	
						
						Integer count = itemUseCnt7Map.get(reasonType);
						if(null == count){
							itemUseCnt7Map.put(reasonType, itemCnt);
						}else{
							itemUseCnt7Map.put(reasonType, itemCnt + count);
						}				
					}
				}
			}else if("ITEMUSE30".equals(val.getSuffix())){
				String[] valFields =  val.getOutFields();
				if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(valFields[0])){
					String[] reasonTypeAndVals = StringUtil.split(valFields[0], ",");
					for(String typeAndVal : reasonTypeAndVals){
						String[] reasonTypeAndVal = StringUtil.split(typeAndVal,":");
						String reasonType = reasonTypeAndVal[0];
						String vals = reasonTypeAndVal[1];
						int itemCnt = StringUtil.convertInt(vals, 0);	
						
						Integer count = itemUseCnt30Map.get(reasonType);
						if(null == count){
							itemUseCnt30Map.put(reasonType, itemCnt);
						}else{
							itemUseCnt30Map.put(reasonType, itemCnt + count);
						}				
					}
				}
			}			
		}
		
		// 道具购买7天人数			
		outputItemBuyPlayerNum(Constants.SUFFIX_ITEM_BUY_SUM_7,Constants.DIMENSION_ITEM_BUY_PLAYERNUM,
				itemBuyPlayerNum7Map,key,valObj,context);
		// 道具购买30天人数			
		outputItemBuyPlayerNum(Constants.SUFFIX_ITEM_BUY_SUM_30,Constants.DIMENSION_ITEM_BUY_PLAYERNUM,
				itemBuyPlayerNum30Map,key,valObj,context); 
		
		// 道具购买7天数量			
		outputItemBuyCnt(Constants.SUFFIX_ITEM_BUY_SUM_7,Constants.DIMENSION_ITEM_BUY_NUM,itemBuyCnt7Map,
				 key, valObj, context);		
		// 道具购买30天数量			
		outputItemBuyCnt(Constants.SUFFIX_ITEM_BUY_SUM_30,Constants.DIMENSION_ITEM_BUY_NUM,itemBuyCnt30Map,
				 key, valObj, context);
		
		// 道具购买7天金额	
		outputItemBuyVirCur(Constants.SUFFIX_ITEM_BUY_SUM_7,Constants.DIMENSION_ITEM_BUY_COIN_VALUE,itemBuyVirCnt7Map,
				 key, valObj, context);
		// 道具购买30天金额	
		outputItemBuyVirCur(Constants.SUFFIX_ITEM_BUY_SUM_30,Constants.DIMENSION_ITEM_BUY_COIN_VALUE,itemBuyVirCnt30Map,
				 key, valObj, context);
		
		// 道具获得7天人数
		outputItemGetPlayerNum(Constants.SUFFIX_ITEM_GET_SUM_7,Constants.DIMENSION_ITEM_SYS_OUTPUT_PLAYERNUM,totalPlayer7,
				key,valObj,context);
		// 道具获得30天人数
		outputItemGetPlayerNum(Constants.SUFFIX_ITEM_GET_SUM_30,Constants.DIMENSION_ITEM_SYS_OUTPUT_PLAYERNUM,totalPlayer30,
				key,valObj,context);
		
		
		// 道具获得7天数量
		outputItemGetCnt(Constants.SUFFIX_ITEM_GET_SUM_7,Constants.DIMENSION_ITEM_SYS_OUTPUT_NUM,itemGetCnt7Map,
				key,valObj,context);
		// 道具获得30天数量
		outputItemGetCnt(Constants.SUFFIX_ITEM_GET_SUM_30,Constants.DIMENSION_ITEM_SYS_OUTPUT_NUM,itemGetCnt30Map,
				key,valObj,context);
		// 道具消耗7天数量
		outputItemUseCnt(Constants.SUFFIX_ITEM_USE_SUM_7,Constants.DIMENSION_ITEM_SYS_CONSUME_NUM,itemUseCnt7Map,
				key,valObj,context);
		// 道具消耗30天数量
		outputItemUseCnt(Constants.SUFFIX_ITEM_USE_SUM_30,Constants.DIMENSION_ITEM_SYS_CONSUME_NUM,itemUseCnt30Map,
				key,valObj,context);
		
	}
	// 道具购买7、30天人数	
	private void outputItemBuyPlayerNum(String suffix,String type, Map<String, Integer> itemBuyPlayerNumMap,
				OutFieldsBaseModel key,OutFieldsBaseModel valObj,Context context)throws IOException, InterruptedException{
		key.setSuffix(suffix);	
		String[] outFields = null;
		//道具购买人数
		/*Set<String> currTypeSet = itemBuyCnt7Map.keySet();
		for(String currType : currTypeSet){
			outFields = new String[]{
				type,//type:分布表中的类型
				currType,//vkey:人数统计不需要 vkey
				itemBuyPlayerNumMap.get(currType) + ""
			};
			valObj.setOutFields(outFields);
			context.write(key, valObj);
		}*/
		
		for(Entry<String, Integer> entry : itemBuyPlayerNumMap.entrySet()){
			outFields = new String[]{
				type,//type:分布表中的类型
				entry.getKey(),//币种(currencyType)
				entry.getValue() + "" // 人数
			};
			valObj.setOutFields(outFields);
			context.write(key, valObj);
		}
	}	
	
	// 道具购买7 30天数量
	private void outputItemBuyCnt(String suffix,String type,Map<String, Integer> itemBuyCntMap,
			OutFieldsBaseModel key,OutFieldsBaseModel valObj,Context context)throws IOException, InterruptedException{
		key.setSuffix(suffix);		
		//某种虚拟币购买道具数量
		Set<String> currTypeSet = itemBuyCntMap.keySet();
		String[] outFields = null;
		for(String currType : currTypeSet){
			//某种虚拟币购买道具数量
			outFields = new String[]{
				type,//type:分布表中的类型
				currType,//vkey:币种
				""+itemBuyCntMap.get(currType)
			};
			valObj.setOutFields(outFields);
			context.write(key, valObj);			
		}
	}	
	// 道具购买7 30天金额
	private void outputItemBuyVirCur(String suffix,String type,Map<String, Integer> itemBuyVirCntMap,
			OutFieldsBaseModel key,OutFieldsBaseModel valObj,Context context)throws IOException, InterruptedException{
		key.setSuffix(suffix);		
		//某种虚拟币购买道具数量
		Set<String> currTypeSet = itemBuyVirCntMap.keySet();
		String[] outFields = null;
		for(String currType : currTypeSet){
			//某种虚拟币购买道具数量
			outFields = new String[]{
					type,//type:分布表中的类型
				currType,//vkey:币种
				""+itemBuyVirCntMap.get(currType)
			};
			valObj.setOutFields(outFields);
			context.write(key, valObj);			
		}
	}
	
	// 道具获得7 30天人数
	private void outputItemGetPlayerNum(String suffix,String type,int totalPlayer,
			OutFieldsBaseModel key,OutFieldsBaseModel valObj,Context context)throws IOException, InterruptedException{
		// 获得的总人数
		String[] outFields = new String[]{
			type,//type:分布表中的类型
			"NA",//vkey:人数留空
			totalPlayer+""
		};
		valObj.setOutFields(outFields);
		key.setSuffix(suffix);
		context.write(key, valObj);
	}
	
	// 道具获得7 30天数量
	private void outputItemGetCnt(String suffix,String type,Map<String, Integer> itemGetCntMap,
			OutFieldsBaseModel key,OutFieldsBaseModel valObj,Context context)throws IOException, InterruptedException{
		key.setSuffix(suffix);		
		Set<String> reasonTypeSet = itemGetCntMap.keySet();
		String[] outFields = null;
		for(String reasonType : reasonTypeSet){
			outFields = new String[]{
				type,//type:分布表中的类型
				reasonType,//vkey:
				""+itemGetCntMap.get(reasonType)
			};
			valObj.setOutFields(outFields);
			context.write(key, valObj);			
		}
	}
	
	// 道具消耗 7 30天数量
	private void outputItemUseCnt(String suffix,String type,Map<String, Integer> itemUseCntMap,
			OutFieldsBaseModel key,OutFieldsBaseModel valObj,Context context)throws IOException, InterruptedException{
		key.setSuffix(suffix);		
		Set<String> reasonTypeSet = itemUseCntMap.keySet();
		String[] outFields = null;
		for(String reasonType : reasonTypeSet){
			outFields = new String[]{
				type,//type:分布表中的类型
				reasonType,//vkey:
				""+itemUseCntMap.get(reasonType)
			};
			valObj.setOutFields(outFields);
			context.write(key, valObj);			
		}
	}

}
