package net.digitcube.hadoop.mapreduce.taskanditem.item;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.PlayerType;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.google.common.reflect.TypeToken;

public class ItemSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private static final TypeToken<Map<String, Integer>> token = new TypeToken<Map<String, Integer>>(){
		private static final long serialVersionUID = 5936101311572176643L;
	};
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private IntWritable mapValObj = new IntWritable(1);
	
	// 当前输入的文件后缀
	private String fileSuffix = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		if(fileSuffix.endsWith(Constants.SUFFIX_ITEM_BUY_FOR_PLAYER)){// 道具购买
			if(array.length < 10){
				return;
			}
			int i = 0;
			String appId = array[i++];
			String platform = array[i++];
			String gameServer = array[i++];
			String accountId = array[i++];
			String itemId = array[i++];
			String itemType = array[i++];
			String channel = array[i++];
			String warpedPlayerType = array[i++]; //玩家类型：新增/活跃/曾经付费
			String curTypeCntStr = array[i++]; //虚拟币类型对应购买数量
			String curTypeAmountStr = array[i++]; //虚拟币类型对应金额
			
			// 道具购买总人数(按新增、活跃、付费玩家统计)
			writeItemInfo(context, appId, platform, channel, gameServer, warpedPlayerType,
					Constants.DIMENSION_ITEM_BUY_PLAYERNUM, // type
					itemId, // vkey1
					itemType, // vkey1
					"NA",//vkey 2:人数留空
					1 // 人数
			);
			
			// 道具购买按虚拟币种统计数量/金额/人数(按新增、活跃、付费玩家统计)
			Map<String, Integer> curTypeCntMap = StringUtil.getMapFromJson(curTypeCntStr, token);
			if(null == curTypeCntMap){
				return;
			}
			Map<String, Integer> curTypeAmountMap = StringUtil.getMapFromJson(curTypeAmountStr, token);
			
			for(Entry<String, Integer> entry : curTypeCntMap.entrySet()){
				//虚拟币购买道具人数
				writeItemInfo(context, appId, platform, channel, gameServer, warpedPlayerType,
						Constants.DIMENSION_ITEM_BUY_PLAYERNUM, // type
						itemId, // vkey1
						itemType, // vkey1
						entry.getKey(), // 虚拟币类型
						1 // 购买人数
				);
				//虚拟币购买道具数量
				writeItemInfo(context, appId, platform, channel, gameServer, warpedPlayerType,
						Constants.DIMENSION_ITEM_BUY_NUM, // type
						itemId, // vkey1
						itemType, // vkey1
						entry.getKey(), // 虚拟币类型
						entry.getValue() // 购买数量
				);
				//虚拟币购买道具金额
				if(null != curTypeAmountMap && null != curTypeAmountMap.get(entry.getKey())){
					Integer coinCount = curTypeAmountMap.get(entry.getKey());
					writeItemInfo(context, appId, platform, channel, gameServer, warpedPlayerType,
							Constants.DIMENSION_ITEM_BUY_COIN_VALUE, // type
							itemId, // vkey1
							itemType, // vkey1
							entry.getKey(), // 虚拟币类型
							coinCount // 虚拟币购买金额
					);
				}
			}
			
		}else if(fileSuffix.endsWith(Constants.SUFFIX_ITEM_GET_FOR_PLAYER)){// 道具获得
			if(array.length < 9){
				return;
			}
			
			int i = 0;
			String appId = array[i++];
			String platform = array[i++];
			String gameServer = array[i++];
			String accountId = array[i++];
			String itemId = array[i++];
			String itemType = array[i++];
			String channel = array[i++];
			String warpedPlayerType = array[i++]; //玩家类型：新增/活跃/曾经付费
			String getReasonCntStr = array[i++]; //虚拟币类型对应购买数量
			
			// 新增、活跃、付费玩家道具获得总人数
			writeItemInfo(context, appId, platform, channel, gameServer, warpedPlayerType,
					Constants.DIMENSION_ITEM_SYS_OUTPUT_PLAYERNUM, // type
					itemId, // vkey1
					itemType, // vkey1
					"NA",//vkey 2:人数留空
					1 // 人数
			);
			
			// 道具获得方式数量分布(按玩家类型统计)
			Map<String, Integer> getReasonCntMap = StringUtil.getMapFromJson(getReasonCntStr, token);
			if(null == getReasonCntMap){
				return;
			}
			for(Entry<String, Integer> entry : getReasonCntMap.entrySet()){
				//道具获得方式数量分布
				writeItemInfo(context, appId, platform, channel, gameServer, warpedPlayerType,
						Constants.DIMENSION_ITEM_SYS_OUTPUT_NUM, // type
						itemId, // vkey1
						itemType, // vkey1
						entry.getKey(), // 道具获得方式
						entry.getValue() // 道具获得数量
				);
			}
		}else if(fileSuffix.endsWith(Constants.SUFFIX_ITEM_USE_FOR_PLAYER)){// 道具消耗
			if(array.length < 9){
				return;
			}
			
			int i = 0;
			String appId = array[i++];
			String platform = array[i++];
			String gameServer = array[i++];
			String accountId = array[i++];
			String itemId = array[i++];
			String itemType = array[i++];
			String channel = array[i++];
			String warpedPlayerType = array[i++]; //玩家类型：新增/活跃/曾经付费
			String useReasonCntStr = array[i++]; //虚拟币类型对应购买数量
			
			// 道具消耗方式数量分布(按玩家类型统计)
			Map<String, Integer> getReasonCntMap = StringUtil.getMapFromJson(useReasonCntStr, token);
			if(null == getReasonCntMap){
				return;
			}
			for(Entry<String, Integer> entry : getReasonCntMap.entrySet()){
				//道具获得方式数量分布
				writeItemInfo(context, appId, platform, channel, gameServer, warpedPlayerType,
						Constants.DIMENSION_ITEM_SYS_CONSUME_NUM, // type
						itemId, // vkey1
						itemType, // vkey1
						entry.getKey(), // 道具获得方式
						entry.getValue() // 道具获得数量
				);
			}
		}
	}
	
	private void writeItemInfo(Context context,
			String appId,
			String platform,
			String channel,
			String gameServer,
			String wrapedPlayerType,
			String type,
			String itemId, // vkey1
			String itemType, // vkey1
			String vkey2, // vkey2
			int count) throws IOException, InterruptedException{
		
		String[] keyFields = new String[]{
				appId,
				platform,
				channel,
				gameServer,
				Constants.PLAYER_TYPE_ONLINE,
				type, // type
				itemId, // vkey1
				itemType, // vkey1
				vkey2//vkey2
		};
		
		mapValObj.set(count);
		mapKeyObj.setOutFields(keyFields);
		
		PlayerType player = new PlayerType(StringUtil.convertInt(wrapedPlayerType, 0));
		//A. 活跃玩家：道具购买总人数
		if(player.isOnline()){
			keyFields[4] = Constants.PLAYER_TYPE_ONLINE; // 活跃玩家
			context.write(mapKeyObj, mapValObj);
		}
		
		//B. 新增玩家：道具购买总人数
		if(player.isNewAdd()){
			keyFields[4] = Constants.PLAYER_TYPE_NEWADD; // 活跃玩家
			context.write(mapKeyObj, mapValObj);
		}
		//C. 曾经付费玩家：道具购买总人数
		if(player.isEverPay()){
			// 曾经付费玩家这里类型这里应该用 PLAYER_TYPE_EVER_PAY
			// 但由于历史数据兼容，这里继续使用 PLAYER_TYPE_PAYMENT
			keyFields[4] = Constants.PLAYER_TYPE_PAYMENT; // 曾经付费玩家
			context.write(mapKeyObj, mapValObj);
		}
	}
}
