package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemBuySumReducer extends Reducer<OutFieldsBaseModel, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	private Map<String, Integer> itemCntMap = new HashMap<String, Integer>();
	private Map<String, Integer> virCurrencyMap = new HashMap<String, Integer>();
	
	private Map<String, Integer> itemBuyPlayerNumMap = new HashMap<String, Integer>();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		itemCntMap.clear();
		virCurrencyMap.clear();		
		itemBuyPlayerNumMap.clear();
		
		int totalPlayer = 0;
		
		for(Text val : values){
			totalPlayer++;
			String[] currTypeAndVals = val.toString().split(",");
			for(String typeAndVal : currTypeAndVals){				
				String[] currTypeAndVal = typeAndVal.split(":");
				String currencyType = currTypeAndVal[0];
				String[] vals = currTypeAndVal[1].split("\\|");
				int itemCnt = StringUtil.convertInt(vals[0], 0);
				int virCurrency = StringUtil.convertInt(vals[1], 0);
				
				Integer count = itemCntMap.get(currencyType);
				if(null == count){
					itemCntMap.put(currencyType, itemCnt);
				}else{
					itemCntMap.put(currencyType, itemCnt + count);
				}
				
				Integer currency = virCurrencyMap.get(currencyType);
				if(null == currency){
					virCurrencyMap.put(currencyType, virCurrency);
				}else{
					virCurrencyMap.put(currencyType, virCurrency + currency);
				}
				
				// 币种人数统计
				Integer num = itemBuyPlayerNumMap.get(currencyType);
				if(null == num){
					itemBuyPlayerNumMap.put(currencyType, 1);
				}else{
					itemBuyPlayerNumMap.put(currencyType, num + 1);
				}
			}
		}
		
		String[] outFields = null;
		//SET SUFFIX
		key.setSuffix(Constants.SUFFIX_ITEM_BUY_SUM);
		//道具购买人数
		Set<String> currTypeSet = itemBuyPlayerNumMap.keySet();
		for(String currType : currTypeSet){
			outFields = new String[]{
				Constants.DIMENSION_ITEM_BUY_PLAYERNUM,//type:分布表中的类型
				currType,//vkey:人数统计不需要 vkey
				itemBuyPlayerNumMap.get(currType) + ""
			};
			valObj.setOutFields(outFields);
			context.write(key, valObj);
		}
		
		// 道具购买总人数
		outFields = new String[]{
				Constants.DIMENSION_ITEM_BUY_PLAYERNUM,
				"NA",//vkey:人数留空
				totalPlayer+""
		};
		valObj.setOutFields(outFields);
		context.write(key, valObj);
		
		//某种虚拟币购买道具数量及金额
		currTypeSet = itemCntMap.keySet();
		for(String currType : currTypeSet){
			//某种虚拟币购买道具数量
			outFields = new String[]{
				Constants.DIMENSION_ITEM_BUY_NUM,//type:分布表中的类型
				currType,//vkey:币种
				""+itemCntMap.get(currType)
			};
			valObj.setOutFields(outFields);
			context.write(key, valObj);
			
			//某种虚拟币购买道具金额
			outFields = new String[]{
				Constants.DIMENSION_ITEM_BUY_COIN_VALUE,//type:分布表中的类型
				currType,//vkey:币种
				""+virCurrencyMap.get(currType)
			};
			valObj.setOutFields(outFields);
			context.write(key, valObj);
		}
	}
}
