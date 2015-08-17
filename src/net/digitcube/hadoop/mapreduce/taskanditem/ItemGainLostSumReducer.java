package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 */

public class ItemGainLostSumReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	private Map<String, Long> itemGetReasonMap = new HashMap<String, Long>();
	private Map<String, Long> itemUseReasonMap = new HashMap<String, Long>();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		itemGetReasonMap.clear();
		itemUseReasonMap.clear();
		
		int totalPlayer = 0;
		for(OutFieldsBaseModel val : values){						
			if("GET".equals(val.getSuffix())){
				String itemGetRedords = val.getOutFields()[0];
				//统计产出人数、产出方式及数量
				if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(itemGetRedords)){
					//只计算道具产出的人数，不算消耗的人数, 只要产出有记录，则人数 +1
					totalPlayer++;
					
					String[] itemGetRedordsArr = itemGetRedords.split(",");
					for(String itemGetReasonCount : itemGetRedordsArr){
						String[] reasonCount = itemGetReasonCount.split(":");
						if(reasonCount.length<2){
							continue;
						}
						
						String reason = reasonCount[0];
						long itemCnt = StringUtil.convertLong(reasonCount[1], 0);
						
						Long count = itemGetReasonMap.get(reason);
						if(null == count){
							itemGetReasonMap.put(reason, itemCnt);
						}else{
							itemGetReasonMap.put(reason, itemCnt + count);
						}
					}
				}
			}else if("USE".equals(val.getSuffix())){
				String itemUseRedords = val.getOutFields()[0];
				//统计消耗方式及数量
				if(!ItemGainLostForPlayerMapper.NO_RESULT.equals(itemUseRedords)){
					String[] itemUseRedordsArr = itemUseRedords.split(",");
					for(String itemUseReasonCount : itemUseRedordsArr){
						String[] reasonCount = itemUseReasonCount.split(":");
						if(reasonCount.length<2){
							continue;
						}
						
						String reason = reasonCount[0];
						long itemCnt = StringUtil.convertLong(reasonCount[1], 0);
						
						Long count = itemUseReasonMap.get(reason);
						if(null == count){
							itemUseReasonMap.put(reason, itemCnt);
						}else{
							itemUseReasonMap.put(reason, itemCnt + count);
						}
					}
				}
			}			
			
		}
		
		//TODO：为了兼容产出及购买中消耗都能展示问题，消耗的数据需入库两次，所以这里需按后缀分开输出
		//key.setSuffix(Constants.SUFFIX_ITEM_GAINLOST_SUM);
		
		// 获得的总人数
		String[] outFields = new String[]{
			Constants.DIMENSION_ITEM_SYS_OUTPUT_PLAYERNUM,//type:分布表中的类型
			"NA",//vkey:人数留空
			totalPlayer+""
		};
		valObj.setOutFields(outFields);
		key.setSuffix(Constants.DIMENSION_ITEM_SYS_OUTPUT_PLAYERNUM);
		context.write(key, valObj);
		
		//产出方式
		Set<String> gainReasonSet = itemGetReasonMap.keySet();
		for(String reason : gainReasonSet){
			//某道具在产出方式上的分布
			outFields = new String[]{
				Constants.DIMENSION_ITEM_SYS_OUTPUT_NUM,//type:分布表中的类型
				reason,//vkey:币种
				""+itemGetReasonMap.get(reason)
			};
			valObj.setOutFields(outFields);
			key.setSuffix(Constants.DIMENSION_ITEM_SYS_OUTPUT_NUM);
			context.write(key, valObj);
		}
		
		//消耗方式
		Set<String> lostReasonSet = itemUseReasonMap.keySet();
		for(String reason : lostReasonSet){
			//某道具在消耗方式上的分布
			outFields = new String[]{
				Constants.DIMENSION_ITEM_SYS_CONSUME_NUM,//type:分布表中的类型
				reason,//vkey:币种
				""+itemUseReasonMap.get(reason)
			};
			valObj.setOutFields(outFields);
			key.setSuffix(Constants.DIMENSION_ITEM_SYS_CONSUME_NUM);
			context.write(key, valObj);
		}
	}
}
