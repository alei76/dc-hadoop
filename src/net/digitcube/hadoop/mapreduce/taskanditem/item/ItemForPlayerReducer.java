package net.digitcube.hadoop.mapreduce.taskanditem.item;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.guanka.GuanKaForPlayerNewReducer;
import net.digitcube.hadoop.mapreduce.hbase.PlayerNewAddInfoHBaseMapper;
import net.digitcube.hadoop.mapreduce.hbase.util.HbasePool;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.PlayerType;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemForPlayerReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	// 道具购买：虚拟币类型-->购买数量
	private Map<String, Integer> curTypeCntMap = new HashMap<String, Integer>();
	// 道具购买：虚拟币类型-->购买总额度
	private Map<String, Integer> curTypeAmountMap = new HashMap<String, Integer>();
	// 道具获得：方式-->数量
	private Map<String, Integer> getReasonCntMap = new HashMap<String, Integer>();
	// 道具消耗：方式-->数量
	private Map<String, Integer> useReasonCntMap = new HashMap<String, Integer>();
	
	private int statTime = 0;
	HConnection conn = null;
	HTableInterface htable = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statTime = DateUtil.getStatDate(context.getConfiguration());
		conn = HbasePool.getConnection(); 
		htable = conn.getTable(PlayerNewAddInfoHBaseMapper.TB_USER_NEWADD);
	}
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		HbasePool.close(conn);
	}
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		curTypeCntMap.clear();
		curTypeAmountMap.clear();
		getReasonCntMap.clear();
		useReasonCntMap.clear();
		
		String appId = key.getOutFields()[0]; 
		String platform = key.getOutFields()[1];
		String gameServer = key.getOutFields()[2];
		String accountId = key.getOutFields()[3];
		PlayerType playerType = GuanKaForPlayerNewReducer.searchPlayerTypeFromHBase(
				htable, 
				appId, 
				platform, 
				gameServer, 
				accountId, 
				statTime
		);
		playerType.markOnline();
		
		if(ItemForPlayerMapper.DATA_FLAG_ITEM_BUY.equals(key.getSuffix())){
			String channel = null;
			for(OutFieldsBaseModel val : values){
				String[] valFields = val.getOutFields();
				int i = 0;
				String tmpChannel = valFields[i++];
				if(null == channel){
					channel = tmpChannel;
				}
				String currencyType = valFields[i++];
				int itemCnt = StringUtil.convertInt(valFields[i++], 0);
				int virCurrency = StringUtil.convertInt(valFields[i++], 0);
				Integer oldCount = curTypeCntMap.get(currencyType);
				if(null == oldCount){
					curTypeCntMap.put(currencyType, itemCnt);
				}else{
					curTypeCntMap.put(currencyType, itemCnt + oldCount);
				}
				
				Integer oldCurrency = curTypeAmountMap.get(currencyType);
				if(null == oldCurrency){
					curTypeAmountMap.put(currencyType, virCurrency);
				}else{
					curTypeAmountMap.put(currencyType, virCurrency + oldCurrency);
				}
			}
			
			String curTypeCntStr = StringUtil.getJsonStr(curTypeCntMap);
			String curTypeAmountStr = StringUtil.getJsonStr(curTypeAmountMap);
			String[] valFields = new String[]{
					channel,
					playerType.toString(), //玩家类型：新增/活跃/曾经付费
					curTypeCntStr, //虚拟币类型对应购买数量
					curTypeAmountStr //虚拟币类型对应金额
			};
			
			valObj.setOutFields(valFields);
			key.setSuffix(Constants.SUFFIX_ITEM_BUY_FOR_PLAYER);
			context.write(key, valObj);
			
		}else if(ItemForPlayerMapper.DATA_FLAG_ITEM_GET.equals(key.getSuffix())){
			String channel = null;
			for(OutFieldsBaseModel val : values){
				int i = 0;
				String[] valFields = val.getOutFields();
				String tmpChannel = valFields[i++];
				if(null == channel){
					channel = tmpChannel;
				}
				String reason = valFields[i++];
				int itemCnt = StringUtil.convertInt(valFields[i++], 0);
				Integer oldCount = getReasonCntMap.get(reason);
				if(null == oldCount){
					getReasonCntMap.put(reason, itemCnt);
				}else{
					getReasonCntMap.put(reason, itemCnt + oldCount);
				}
			}
			
			String getReasonCntStr = StringUtil.getJsonStr(getReasonCntMap);
			String[] valFields = new String[]{
					channel,
					playerType.toString(), //玩家类型：新增/活跃/曾经付费
					getReasonCntStr //获得方式及数量
			};
			
			valObj.setOutFields(valFields);
			key.setSuffix(Constants.SUFFIX_ITEM_GET_FOR_PLAYER);
			context.write(key, valObj);
			
		}else if(ItemForPlayerMapper.DATA_FLAG_ITEM_USE.equals(key.getSuffix())){
			String channel = null;
			for(OutFieldsBaseModel val : values){
				int i = 0;
				String[] valFields = val.getOutFields();
				String tmpChannel = valFields[i++];
				if(null == channel){
					channel = tmpChannel;
				}
				String reason = valFields[i++];
				int itemCnt = StringUtil.convertInt(valFields[i++], 0);
				Integer oldCount = useReasonCntMap.get(reason);
				if(null == oldCount){
					useReasonCntMap.put(reason, itemCnt);
				}else{
					useReasonCntMap.put(reason, itemCnt + oldCount);
				}
			}
			
			String useReasonCntStr = StringUtil.getJsonStr(useReasonCntMap);
			String[] valFields = new String[]{
					channel,
					playerType.toString(), //玩家类型：新增/活跃/曾经付费
					useReasonCntStr //使用方式及数量
			};
			
			valObj.setOutFields(valFields);
			key.setSuffix(Constants.SUFFIX_ITEM_USE_FOR_PLAYER);
			context.write(key, valObj);
		}
	}
}
