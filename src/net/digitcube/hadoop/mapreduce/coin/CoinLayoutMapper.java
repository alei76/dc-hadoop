package net.digitcube.hadoop.mapreduce.coin;

import java.io.IOException;
import net.digitcube.hadoop.common.AttrUtil;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <pre>
 * 虚拟币收入、支出、收入方式分布、支出方式分布 Map
 * Title: CoinMapper.java<br>
 * Description: CoinMapper.java<br>
 * Copyright: Copyright (c) 2013 www.dataeye.com<br>
 * Company: DataEye 数据之眼<br>
 * 
 * @author Ivan     <br>
 * @date 2013-11-6         <br>
 * @version 1.0
 * <br>
 * 
 * 输入文件：
 * 依赖自定义事件分离 @EventSeparatorMapper 的结果
 * 后缀为 DESelf_CG_Lost 房间虚拟币消耗
 * 后缀为 DESelf_CG_Gain 房间虚拟币获得
 * 
 * 后缀为 DESelf_Coin_Lost 虚拟币消耗
 * 后缀为 DESelf_Coin_Gain 虚拟币获得
 * 
 * 依赖玩家当天虚拟币总量去重 @TotalCoinEachAccountMapper 的结果
 * 后缀为 TOTAL_COIN_EACH_ACCOUNT 虚拟币留存量
 * 
 * 
 * Map 输出：
 * [ appID, platform, channel, gameServer, id ,Flag,roomid,num ]
 * [ appID, platform, channel, gameServer, id ,Flag,num ]
 * [ appID, platform, channel, gameServer, coinNum ]
 */
public class CoinLayoutMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, Text> {
	public static final String ROOMID = "roomId";
	public static final String ID = "id";
	public static final String NUM = "num";
	public static final String TOTAL = "total";
	public static final String SEQ = "seq";

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	//private IntWritable valObj = new IntWritable();
	private Text valObj = new Text();
	private String fileSuffix;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();		
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String[] keyFields = null;
		String[] keyFields_AllGS = null;
		
		//if (fileSuffix.endsWith(Constants.SUFFIX_TOTAL_COIN_EACH_ACCOUNT)) {
		if(fileSuffix.endsWith(Constants.SUFFIX_COIN_ROLLING)){
			if(paraArr.length < 8){
				return;
			}
			
			// 虚拟币存量
			int i = 0;
			String appID = paraArr[i++];
			String platform = paraArr[i++];
			String channel = paraArr[i++];
			String gameServer = paraArr[i++];
			String accountId = paraArr[i++];
			String coinType = Constants.DEFAULT_COIN_TYPE;
			if(paraArr.length > 7){//20140617 : 旧版本还未加入 coinType
				coinType = paraArr[i++];
			}
			int coinNum = StringUtil.convertInt(paraArr[i++], 0);
			String lastUpdateTime = paraArr[i++];
			

			keyFields = new String[] { appID, platform, channel, gameServer,
					coinType, // type
					Constants.DIMENSION_RETAIN // vkey1
			};
			keyObj.setOutFields(keyFields);
			valObj.set(coinNum+"");
			
			keyObj.setSuffix(Constants.SUFFIX_TOTAL_COIN);
			context.write(keyObj, valObj);
		} 
		/*else if (fileSuffix.endsWith(Constants.SUFFIX_COIN_GAIN_LOST_USERDURE)) {
			if(paraArr.length < 6){
				return;
			}
			
			int i = 0;
			String appID = paraArr[i++];
			String platform = paraArr[i++];
			String channel = paraArr[i++];
			String gameServer = paraArr[i++];
			String accountId = paraArr[i++];
			String coinType = paraArr[i++];
			
			// 虚拟币消费用户数量
			//真实区服
			keyFields = new String[] { appID, platform, channel, gameServer, 
					"NA", // vkey2 人数不许按 key2 统计，留空
					coinType, // type
					Constants.DIMENSION_COIN_LOST_USER_NUM // vkey1
			};
			//全服
			keyFields_AllGS = new String[] { appID, platform, channel, MRConstants.ALL_GAMESERVER, 
					"NA", // vkey2 
					coinType, // type
					Constants.DIMENSION_COIN_LOST_USER_NUM // vkey1
			};
			
			keyObj.setOutFields(keyFields);
			keyObj.setSuffix(Constants.SUFFIX_COIN_GAIN_LOST);
			valObj.set(1);
		}*/ 
		else{
			EventLog eventLog = new EventLog(paraArr);
			String appID = eventLog.getAppID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer = eventLog.getGameServer();
			String accountId = eventLog.getAccountID();
			
			/*String appID = paraArr[1];
			String platform = paraArr[4];
			String channel = paraArr[5];
			String gameServer = paraArr[9];
			String eventAttrString = paraArr[paraArr.length - 1];
			String id = AttrUtil.getEventAttrValue(eventAttrString, ID, "");
			int num = StringUtil.convertInt(AttrUtil.getEventAttrValue(eventAttrString, NUM, ""), 0);*/
			
			//虚拟币消耗或获得的方式
			String id = eventLog.getArrtMap().get(ID);
			if(null == id){
				return;
			}
			int num = StringUtil.convertInt(eventLog.getArrtMap().get(NUM), 0);
			if(num <= 0){
				return;
			}
			valObj.set(num+"");
			
			//20140617 : coinType 在之前的协议中没有，这里需做兼容
			String coinType = eventLog.getArrtMap().get("coinType");
			coinType = null == coinType ? Constants.DEFAULT_COIN_TYPE : coinType;

			if (fileSuffix.endsWith(Constants.DESelf_CG_Lost)) {
				// 虚拟币房间支出
				//String roomid = AttrUtil.getEventAttrValue(eventAttrString, ROOMID, "");
				String roomid = eventLog.getArrtMap().get(ROOMID);
				if(null == roomid){
					return;
				}
				//真实区服
				keyFields = new String[] { appID, platform, channel, gameServer, id, Constants.DIMENSION_ROOM_COIN_LOST, roomid };
				//全服
				keyFields_AllGS = new String[] { appID, platform, channel, MRConstants.ALL_GAMESERVER, id, Constants.DIMENSION_ROOM_COIN_LOST, roomid };
				
				keyObj.setOutFields(keyFields);
				keyObj.setSuffix(Constants.SUFFIX_ROOM_COIN_GAIN_LOST);
				
				//真实区服
				context.write(keyObj, valObj);
				//全服
				keyObj.setOutFields(keyFields_AllGS);
				context.write(keyObj, valObj);
			} else if (fileSuffix.endsWith(Constants.DESelf_CG_Gain)) {
				// 虚拟币房间收入
				//String roomid = AttrUtil.getEventAttrValue(eventAttrString, ROOMID, "");
				String roomid = eventLog.getArrtMap().get(ROOMID);
				if(null == roomid){
					return;
				}
				//真实区服
				keyFields = new String[] { appID, platform, channel, gameServer, id, Constants.DIMENSION_ROOM_COIN_GAIN, roomid };
				//全服
				keyFields_AllGS = new String[] { appID, platform, channel, MRConstants.ALL_GAMESERVER, id, Constants.DIMENSION_ROOM_COIN_GAIN, roomid };
				
				keyObj.setOutFields(keyFields);
				keyObj.setSuffix(Constants.SUFFIX_ROOM_COIN_GAIN_LOST);
				
				//真实区服
				context.write(keyObj, valObj);
				//全服
				keyObj.setOutFields(keyFields_AllGS);
				context.write(keyObj, valObj);
			} else if (fileSuffix.endsWith(Constants.DESelf_Coin_Lost)) {
				// 虚拟币支出
				//真实区服
				keyFields = new String[] { appID, platform, channel, gameServer, 
						id, // vkey2 
						coinType, // type
						Constants.DIMENSION_LOST // vkey1
				};
				//全服
				keyFields_AllGS = new String[] { appID, platform, channel, MRConstants.ALL_GAMESERVER, 
						id, // vkey2 
						coinType, // type
						Constants.DIMENSION_LOST // vkey1
				};
				
				keyObj.setOutFields(keyFields);
				keyObj.setSuffix(Constants.SUFFIX_COIN_GAIN_LOST);
				
				//真实区服
				context.write(keyObj, valObj);
				//全服
				keyObj.setOutFields(keyFields_AllGS);
				context.write(keyObj, valObj);
				
				//A. 虚拟币消耗总人数
				writeCoinLostPlayerNum(context, 
						accountId, 
						appID, 
						platform, 
						channel, 
						gameServer, 
						"NA", // 算总人数时，虚拟币消耗方式用 NA 代替
						coinType // 虚拟币类型
				);
				//B. 虚拟币消耗方式人数分布
				writeCoinLostPlayerNum(context, 
						accountId, 
						appID, 
						platform, 
						channel, 
						gameServer, 
						id, // 算总人数时，虚拟币消耗方式用 NA 代替
						coinType // 虚拟币类型
				);
			} else if (fileSuffix.endsWith(Constants.DESelf_Coin_Gain)) {
				// 虚拟币收入
				//真实区服
				keyFields = new String[] { appID, platform, channel, gameServer, 
						id, // vkey2
						coinType, // type
						Constants.DIMENSION_GAIN  // vkey1
				};
				//全服
				keyFields_AllGS = new String[] { appID, platform, channel, MRConstants.ALL_GAMESERVER, 
						id, // vkey2
						coinType, // type
						Constants.DIMENSION_GAIN  // vkey1
				};
				keyObj.setOutFields(keyFields);
				keyObj.setSuffix(Constants.SUFFIX_COIN_GAIN_LOST);
				
				//真实区服
				context.write(keyObj, valObj);
				//全服
				keyObj.setOutFields(keyFields_AllGS);
				context.write(keyObj, valObj);
			}
		}
		
		/*if (null != keyFields){
			context.write(keyObj, valObj);
		}
		//全服
		if (null != keyFields_AllGS){
			keyObj.setOutFields(keyFields_AllGS);
			context.write(keyObj, valObj);
		}*/
		
	}

	private void writeCoinLostPlayerNum(Context context, 
			String accountId, 
			String appId, 
			String platform, 
			String channel, 
			String gameServer,
			String lostReason, 
			String coinType) throws IOException, InterruptedException{
		
		// 虚拟币消费用户数量
		//真实区服
		String[] keyFields = new String[] { 
				appId, 
				platform, 
				channel, 
				gameServer, 
				lostReason, // vkey2：虚拟币消耗方式
				coinType, // type：虚拟币类型
				Constants.DIMENSION_COIN_LOST_USER_NUM // vkey1
		};
		
		keyObj.setOutFields(keyFields);
		valObj.set(accountId);
		
		keyObj.setSuffix(Constants.SUFFIX_COIN_LOST_PLAYER_NUM);
		//真实区服
		context.write(keyObj, valObj);
		//全服
		keyFields[3] = MRConstants.ALL_GAMESERVER;
		keyObj.setOutFields(keyFields);
		context.write(keyObj, valObj);
	}
}
