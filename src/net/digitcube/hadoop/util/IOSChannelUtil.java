package net.digitcube.hadoop.util;

import java.io.IOException;
import java.util.LinkedHashMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.mapreduce.hbase.util.HbasePool;

public class IOSChannelUtil {
	
	//20141203 ios_channel 表改为 dc_click_channel
	//private static final String HBASE_TABLE_ISO_CHANNEL = "ios_channel";
	private static final String HBASE_TABLE_CHANNEL = "dc_click_channel";
	private static HTableInterface channelTable = null; 
	/*
	 * appId + UID 对应的渠道缓存，减少 HBase 的访问
	 * appIdUidCache 中存放查询过的记录，包括查询成功和不成功的记录
	 * 如果查询成功，则放查询到的渠道，反之放默认渠道
	 * 该缓存对于 @EventSeparatorMapper 和 EventStatics 都比较有效
	 * 而对于 @OnlineDayMapper 和 @PaymentDayMapper 这类按玩家汇总过的 MR 作用没那么明显
	 */
	private static LRULinkedHashMap<String, String> appIdUidCache = new LRULinkedHashMap<String, String>(10000);
	
	/**
	 * 
	 * a) 如果是 android 平台，直接返回  defaultChannel
	 * <br>
	 * b) 只有平台和渠道为  iOS 和 DataEye_IOS_Channel 时,才会从 HBase 中查询渠道
	 * <br>
	 * c) 如果 HBase 查询没有返回结果则返回 defaultChannel, 否则返回查询到的结果
	 * 
	 * @param appId
	 * @param uid
	 * @param platform
	 * @param defaultChannel
	 * @return
	 * @throws IOException
	 */
	public static String checkForiOSChannel(String appId, String uid, String platform, String defaultChannel) throws IOException{
		try{
		
		//20141202 : 出 iOS平台外，其它平台也有渠道匹配需求
		//只有 iOS 平台才有渠道匹配的需要
		/*if(!MRConstants.PLATFORM_iOS_STR.equals(platform)){
			return defaultChannel;
		}*/
		
		//只有默认渠道 Channle_For_Track 才需要匹配
		if(!Constants.Channle_For_Track.equals(defaultChannel)){
			return defaultChannel;
		}
		//appId 可能带有版本号
		String[] appIdAndVersion = appId.split("\\|");
		if(appIdAndVersion.length > 1){
			appId = appIdAndVersion[0];
		}
		String rowKey = appId + "|" + platform + "|" + uid;
		// appIdUidCache 中保存查询过的结果，查询渠道可能存在也可能不存在
		// 存在则放正确渠道，不存在则放默认渠道
		String reviseChannel = appIdUidCache.get(rowKey);
		if(null != reviseChannel){
			return reviseChannel;
		}
		
		//从 HBase iOS 渠道表中查询
		/*Result result = HbaseProxyClient.getOneRecord(HBASE_TABLE_CHANNEL, rowKey);
		if(null == result || result.isEmpty()){
			//不存在则把默认渠道 defaultChannel 放入缓存，下次不必再查询
			appIdUidCache.put(rowKey, defaultChannel);
			return defaultChannel;
		}
		byte[] val = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("ch"));
		reviseChannel = Bytes.toString(val);*/
		reviseChannel = getChannelFromHbase(rowKey, defaultChannel);
		//把查询到结果放入缓存
		if(StringUtil.isEmpty(reviseChannel)){
			return defaultChannel;
		}
		
		appIdUidCache.put(rowKey, reviseChannel);
		return reviseChannel;
		
		}catch(Throwable t){
			t.printStackTrace();
			return defaultChannel;
		}
	}
	
	private static synchronized String getChannelFromHbase(String rowKey, String defaultChannel){
		try{
			if(null == channelTable){
				channelTable = HbasePool.getConnection().getTable(HBASE_TABLE_CHANNEL);			
			}
			
			Get get = new Get(Bytes.toBytes(rowKey));
			Result rs = channelTable.get(get);
			byte[] val = rs.getValue(Bytes.toBytes("info"), Bytes.toBytes("ch"));
			return Bytes.toString(val);
		}catch(Throwable t){
			t.printStackTrace();
			return defaultChannel;
		}
	}
	
	public static void close(){
		try{
			HbasePool.close(channelTable);
		}catch(Throwable t){
			t.printStackTrace();
		}
		
		try{
			HbasePool.closeAllConns();
		}catch(Throwable t){
			t.printStackTrace();
		}
	}
	
	/**
	 * key=APPID|UID, value=channel 的简单缓存，减少 HBase 的访问次数 
	 * 该缓存对于 @EventSeparatorMapper 应该比较有效
	 * 而对于 @OnlineDayMapper 和 @PaymentDayMapper 这些按玩家汇总过的 MR 效果不是太明显
	 * 
	 * @param <K>
	 * @param <V>
	 */
	private static class LRULinkedHashMap<K, V> extends LinkedHashMap<K, V> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -4786748757297572154L;
		
		private final int maxCapacity;  
	    private static final float DEFAULT_LOAD_FACTOR = 0.75f;  
	    public LRULinkedHashMap(int maxCapacity) {  
	        super(maxCapacity, DEFAULT_LOAD_FACTOR, true);  
	        this.maxCapacity = maxCapacity;  
	    }
	    
	    @Override  
	    protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {  
	        return size() > maxCapacity;  
	    }
	}
}
