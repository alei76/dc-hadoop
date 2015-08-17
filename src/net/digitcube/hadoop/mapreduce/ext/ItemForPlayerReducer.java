package net.digitcube.hadoop.mapreduce.ext;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemForPlayerReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {
	
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
		
		int j = 0;
		String appId = key.getOutFields()[j++]; 
		String platform = key.getOutFields()[j++];
		String gameServer = key.getOutFields()[j++];
		String accountId = key.getOutFields()[j++];
		String itemId = key.getOutFields()[j++];
		PlayerType playerType = GuanKaForPlayerNewReducer.searchPlayerTypeFromHBase(
				htable, 
				appId, 
				platform, 
				gameServer, 
				accountId, 
				statTime
		);
		playerType.markOnline();
		
		String maxVer = "";
		String uid = null;
		String channel = null;
		String itemType = null;
		int buyCount = 0;
		int getCount = 0;
		int useCount = 0;
		for(OutFieldsBaseModel val : values){
			String[] valFields = val.getOutFields();
			int i = 0;
			String tmpVer = valFields[i++];
			String tmpUid = valFields[i++];
			String tmpChannel = valFields[i++];
			String tmpItemType = valFields[i++];
			if(tmpVer.compareTo(maxVer) > 0){
				maxVer = tmpVer;
				uid = tmpUid;
				channel = tmpChannel;
				itemType = tmpItemType;
			}
			
			if(ItemForPlayerMapper.DATA_FLAG_ITEM_BUY.equals(val.getSuffix())){
				String currencyType = valFields[i++];
				int itemCnt = StringUtil.convertInt(valFields[i++], 1);
				int virCurrency = StringUtil.convertInt(valFields[i++], 0);
				buyCount += itemCnt;
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
			}else if(ItemForPlayerMapper.DATA_FLAG_ITEM_GET.equals(val.getSuffix())){
				String reason = valFields[i++];
				int itemCnt = StringUtil.convertInt(valFields[i++], 1);
				getCount += itemCnt; 
				Integer oldCount = getReasonCntMap.get(reason);
				if(null == oldCount){
					getReasonCntMap.put(reason, itemCnt);
				}else{
					getReasonCntMap.put(reason, itemCnt + oldCount);
				}
			}else if(ItemForPlayerMapper.DATA_FLAG_ITEM_USE.equals(val.getSuffix())){
				String reason = valFields[i++];
				int itemCnt = StringUtil.convertInt(valFields[i++], 1);
				useCount += itemCnt; 
				Integer oldCount = useReasonCntMap.get(reason);
				if(null == oldCount){
					useReasonCntMap.put(reason, itemCnt);
				}else{
					useReasonCntMap.put(reason, itemCnt + oldCount);
				}
			}
		}
		
		String curTypeCntStr = StringUtil.getJsonStr(curTypeCntMap);
		String curTypeAmountStr = StringUtil.getJsonStr(curTypeAmountMap);
		String getReasonCntStr = StringUtil.getJsonStr(getReasonCntMap);
		String useReasonCntStr = StringUtil.getJsonStr(useReasonCntMap);
		String[] valFields = new String[]{
				appId,
				maxVer,
				platform,
				channel,
				gameServer,
				accountId,
				uid,
				playerType.toString(), //玩家类型：新增/活跃/曾经付费
				itemId,
				itemType,
				buyCount+"", //总购买数量
				getCount+"", //总获得数量
				useCount+"", //总消耗数量
				curTypeCntStr, //虚拟币类型对应购买数量
				curTypeAmountStr, //虚拟币类型对应金额
				getReasonCntStr, //获得方式及数量
				useReasonCntStr //使用方式及数量
		};
		
		key.setOutFields(valFields);
		key.setSuffix(Constants.SUFFIX_ITEM_FOR_PLAYER);
		context.write(key, NullWritable.get());
		
		
	}
}
