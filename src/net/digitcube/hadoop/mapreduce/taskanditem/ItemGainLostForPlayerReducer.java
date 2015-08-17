package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class ItemGainLostForPlayerReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	// 道具获得方式
	private Map<String, Integer> getReasonMap = new HashMap<String, Integer>();
	// 道具消耗
	private Map<String, Integer> useReasonMap = new HashMap<String, Integer>();
	private StringBuilder sb = new StringBuilder();
	
	private int statTime;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statTime = DateUtil.getStatDateForHourOrToday(context.getConfiguration());
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		getReasonMap.clear();
		useReasonMap.clear();
		sb.delete(0, sb.length());
		
		String channel = null;
		String playerType = Constants.DATA_FLAG_PLAYER_ONLINE; // 只是活跃
		String[] valFieldsNewPay = null;
		String firstLoginDate = null;
		String totalPayCur = null;
		String[] valFields = null;
		for(OutFieldsBaseModel val : values){			
			if("GETUSE".equals(val.getSuffix())){					
				if(null == channel){
					channel = val.getOutFields()[0];
				}
				valFields = val.getOutFields();
				String itemId = valFields[1];
				String itemType = valFields[2];
				String getOrUser = valFields[3];
				String reason = valFields[4];
				int itemCnt = StringUtil.convertInt(valFields[5], 0);
				// 以道具和币种为键值
				String groupKey = itemId + "|" + itemType + "|" + reason;
				
				//道具获得（产出）或购买都认为是道具产出
				//201407某日: 与境哥、sandy 重新确认，不再认为道具购买是道具产出的一种
				if(ItemGainLostForPlayerMapper.ITEM_GET.equals(getOrUser)){
					Integer count = getReasonMap.get(groupKey);
					if(null == count){
						getReasonMap.put(groupKey, itemCnt);
					}else{
						getReasonMap.put(groupKey, itemCnt + count);
					}
				}else if(ItemGainLostForPlayerMapper.ITEM_USE.equals(getOrUser)){
					Integer count = useReasonMap.get(groupKey);
					if(null == count){
						useReasonMap.put(groupKey, itemCnt);
					}else{
						useReasonMap.put(groupKey, itemCnt + count);
					}
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
		
		if(StringUtil.isEmpty(firstLoginDate)){
			firstLoginDate = "0";
		}
		if(StringUtil.isEmpty(firstLoginDate)){
			totalPayCur = "0";
		}
		
		//道具产出的方式与数据 records
		Set<Entry<String,Integer>> itemGetReasonSet = getReasonMap.entrySet();
		for(Entry<String,Integer> reason : itemGetReasonSet){
			sb.append(reason.getKey()!=null?reason.getKey().replaceAll("\r", "").replaceAll("\n", "").replaceAll(",", ""):reason.getKey())
			  .append(":")
			  .append(reason.getValue())
			  .append(",");
		}
		String itemGetReasonCount = sb.toString();
		
		//道具消耗的方式与数据 records
		sb.delete(0, sb.length());
		Set<Entry<String,Integer>> itemUseReasonSet = useReasonMap.entrySet();
		for(Entry<String,Integer> reason : itemUseReasonSet){
			sb.append(reason.getKey()!=null?reason.getKey().replaceAll("\r", "").replaceAll("\n", "").replaceAll(",", ""):reason.getKey())
			  .append(":")
			  .append(reason.getValue())
			  .append(",");
		}
		String itemUseReasonCount = sb.toString();
		
		String[] outFields = new String[]{
				channel, // channel
				"".equals(itemGetReasonCount) ? ItemGainLostForPlayerMapper.NO_RESULT : itemGetReasonCount, // [itemId|itemType|getReason:sum(itemCnt),...]
				"".equals(itemUseReasonCount) ? ItemGainLostForPlayerMapper.NO_RESULT : itemUseReasonCount,  // [itemId|itemType|useReason:sum(itemCnt),...]
				firstLoginDate,
				totalPayCur
		};
		valObj.setOutFields(outFields);
		key.setSuffix(Constants.SUFFIX_ITEM_GAINLOST_PLAYER);
		String[] keyArray = key.getOutFields();		
		key.setOutFields(new String[]{keyArray[0],keyArray[1],keyArray[2],keyArray[3],playerType});	
		context.write(key, valObj);
	}
	
}
