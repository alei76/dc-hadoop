package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class ItemBuyForPlayerReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	private Map<String, Integer> itemCntMap = new HashMap<String, Integer>();
	private Map<String, Integer> virCurrencyMap = new HashMap<String, Integer>();
	private StringBuilder sb = new StringBuilder();
	
	private int statTime;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statTime = DateUtil.getStatDateForHourOrToday(context.getConfiguration());
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		itemCntMap.clear();
		virCurrencyMap.clear();
		sb.delete(0, sb.length());
		
		String playerType = Constants.DATA_FLAG_PLAYER_ONLINE; // 只是活跃
		
		String[] valFields = null;
		String[] valFieldsNewPay = null;
		String firstLoginDate = null;
		String totalPayCur = null;
		for(OutFieldsBaseModel val : values){			
			if("ITEMBUY".equals(val.getSuffix())){				
				valFields = val.getOutFields();
				String itemId = valFields[1];
				String itemType = valFields[2];
				String currencyType = valFields[3];
				int itemCnt = StringUtil.convertInt(valFields[4], 0);
				int virCurrency = StringUtil.convertInt(valFields[5], 0);
				// 以道具和币种为键值
				String groupKey = itemId + "|" + itemType + "|" + currencyType;
				Integer count = itemCntMap.get(groupKey);
				if(null == count){
					itemCntMap.put(groupKey, itemCnt);
				}else{
					itemCntMap.put(groupKey, itemCnt + count);
				}
				
				Integer currency = virCurrencyMap.get(groupKey);
				if(null == currency){
					virCurrencyMap.put(groupKey, virCurrency);
				}else{
					virCurrencyMap.put(groupKey, virCurrency + currency);
				}
				
			}else if("PLAYERNEWPAY".equals(val.getSuffix())){				
				valFieldsNewPay = val.getOutFields();
				firstLoginDate = valFieldsNewPay[0];
				totalPayCur = valFieldsNewPay[1];
				boolean isNewPlayer = statTime == StringUtil.convertInt(firstLoginDate, 0);
				boolean isPayToday = StringUtil.convertInt(totalPayCur, 0) > 0;
				if (isNewPlayer && isPayToday) { // 集新增、活跃、付费于一身
					playerType = Constants.DATA_FLAG_PLAYER_NEW_ONLINE_PAY;
				} else if (isNewPlayer) { // 只是新增、活跃
					playerType = Constants.DATA_FLAG_PLAYER_NEW_ONLINE;
				} else if (isPayToday) { // 只是活跃、付费
					playerType = Constants.DATA_FLAG_PLAYER_ONLINE_PAY;
				}				
			}
		}
		if(null == valFields){
			return;
		}
		Set<String> groupKeySet = itemCntMap.keySet();
		for(String groupKey : groupKeySet){
			sb.append(groupKey)
			  .append(":")
			  .append(itemCntMap.get(groupKey))
			  .append("|")
			  .append(virCurrencyMap.get(groupKey))
			  .append(","); 
		}
		String[] outFields = new String[]{
				valFields[0], // channel
				sb.toString(), // [itemId|itemType|currencyType:sum(itemCnt)|sum(virCurrency),]
				StringUtil.convertInt(firstLoginDate, 0) + "",
				StringUtil.convertInt(totalPayCur, 0) + ""
		};
		valObj.setOutFields(outFields);
		key.setSuffix(Constants.SUFFIX_ITEM_BUY_PLAYER);
		String[] keyArray = key.getOutFields();		
		key.setOutFields(new String[]{keyArray[0],keyArray[1],keyArray[2],keyArray[3],playerType});		
		context.write(key, valObj);
	}
}
