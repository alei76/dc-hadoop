package net.digitcube.hadoop.mapreduce.onlinenew;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author rickpan
 * @version 1.0 
 * 
 * 主要逻辑：
 * 以玩家每小时同一次登录去重后的结果作为输入(@OnlineHourMapper)
 * 同时统计该时间片的 在线人数、ACU、PCU
 * 在线人数：
 * map:
 * 		key : APPID,Platform,GameServer,AccountID,'CNT'(在线人数字符串标志)
 * 		value : 1
 * reduce :
 * 		key : APPID,Platform,GameServer,AccountID,'CNT'
 * 		value : 1
 * 
 * ACU:
 * map:
 * 		key : APPID,Platform,GameServer,TimePoint,'ACU'(字符串标志)
 * 		value : 1
 * reduce :
 * 		key : APPID,Platform,GameServer,TimePoint,'ACU'
 * 		value : sum(1) 
 * 
 * PCU:
 * map:
 * 		key : APPID,Platform,GameServer,TimePoint,'PCU'(字符串标志)
 * 		value : 1
 * reduce :
 * 		key : APPID,Platform,GameServer,TimePoint,'PCU'
 * 		value : sum(1) 
 */

public class AcuPcuCntMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	
	// timePointArr = {10:00,10:05,10:10,...,10:50,10:55,11:00}
	private int[] timePointArr = new int[13];
	
	/**
	 * 从 2013.12.05 开始，所有指标都分为全服算和分区服算
	 * 这里不分区服 "不分区服的统计，gameServer 以 '-' 代替" 在 OnlineHour 中已经有全服计算:_ALL_GS
	 * 所以这里 ACU/PCU/CNT 直接用区服算即可
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		Calendar calendar = Calendar.getInstance();
		Date date = ConfigManager.getInitialDate(context.getConfiguration());
		if (date != null) {
			calendar.setTime(date);
		}
		calendar.add(Calendar.HOUR_OF_DAY, -1); // 默认取调度初始化时间的前一个小时
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		
		// 5 分钟为一个步长的时间点数组,保存当前计算小时的所有时间点以及下一小时 0 点 0 分 0 秒的时间点
		// 下一小时 0 点 0 分 0 秒的时间点只用于计算判断，该时间点不会输出统计结果
		int startPoint = (int)(calendar.getTimeInMillis()/1000);// 起始时间点
		for (int i = 0; i < 13; i++) {
			timePointArr[i] = startPoint + i * 5 * 60;// 5分钟为步长
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] valueArr = value.toString().split(MRConstants.SEPERATOR_IN);
		if(valueArr.length < 20){
			return;
		}
		String appId = valueArr[Constants.INDEX_APPID];
		String platform = valueArr[Constants.INDEX_PLATFORM];
		String gameServer = valueArr[Constants.INDEX_GAMESERVER];
		
		int loginTime = StringUtil.convertInt(valueArr[Constants.INDEX_LOGIN_TIME], 0);
		int onlineTime = StringUtil.convertInt(valueArr[Constants.INDEX_ONLINE_TIME], 0);
		
		/*
		 * 该时间片在线玩家去重统计
		 * 输入是对同一个玩家同一次登录的去重结果，但一个玩家在同一小时内会有多次登录
		 * 所以在 reduce 不能 sum，只输出 1 即可
		 */
		String accountId = valueArr[Constants.INDEX_ACCOUNTID];
		processOnlineCount(accountId, appId, platform, gameServer, context);
		
		// 如果'登录时间  + 在线时长' 小于当前小时的开始或者登录时间当前小时的最末，则不计算 ACU/PCU
		if((loginTime + onlineTime) < timePointArr[0] || loginTime > timePointArr[timePointArr.length - 1]){
			return;
		}
		
		/*
		 * 计算输出 ACU(同一条记录会输出多行)
		 * 每 5 分钟为一次步长进行计算输出
		 */
		processACU(loginTime, onlineTime, appId, platform, gameServer, context);
		
		/*
		 * 计算输出 PCU(同一条记录会输出多行)
		 * 每秒钟为一次步长进行计算输出
		 */
		//PCU 计算逻辑改为取最大 ACU
		//processPCU(loginTime, onlineTime, timePointArr[0], appId, platform, gameServer, context);
	}
	
	private void processACU(int loginTime, int onlineTime, 
			String appId, String platform, String gameServer, 
			Context context) throws IOException, InterruptedException{
		
		// 找出登录时间所在时间点的位置
		int timePointIndex = getTimePointIndex(loginTime);
		int lastTimePointIndex = timePointIndex == 0 ? timePointIndex : timePointIndex - 1;
		
		// 不算最后一个时间点，最后一个时间点是下一个小时的开始
		int endIndex = timePointArr.length - 1;
		for(int i = lastTimePointIndex; i < endIndex; i++){
			
			//从登录时间所在的最小时间点开始，往后取各个时间点（5 分钟步长）计算
			//如果 登录时间+在线时间 大于该时间点，说明玩家在线时间跨越该时间点
			//把该时间点的统计输出
			int timePoint = timePointArr[i];
			if((loginTime + onlineTime) >= timePoint){
				
				String[] keyFields = new String[]{
						appId, 
						platform, 
						gameServer, 
						"" + timePoint,
						Constants.DATA_FLAG_ACU
				};
				mapKeyObj.setOutFields(keyFields);
				context.write(mapKeyObj, one);

				/*// 分区服的统计
				if(!StringUtil.isEmpty(gameServer) && !MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(gameServer.trim())){
					String[] keyFields = new String[]{appId, 
													  platform, 
													  gameServer, 
													  "" + timePoint,
													  Constants.DATA_FLAG_ACU};
					mapKeyObj.setOutFields(keyFields);
					context.write(mapKeyObj, one);
				}
				
				// 不分区服的统计，gameServer 以 '-' 代替
				String[] keyFieldsAll = new String[] {appId, 
													  platform,
													  MRConstants.INVALID_PLACE_HOLDER_CHAR,
													  "" + timePoint,
													  Constants.DATA_FLAG_ACU};
				mapKeyObj.setOutFields(keyFieldsAll);
				context.write(mapKeyObj, one);*/
			}
		}
	}
	
	private void processPCU(int loginTime, int onlineTime, int startPoint, 
							String appId, String platform, String gameServer, 
							Context context) throws IOException, InterruptedException{
		
		int maxPoint = startPoint + 3600; // 最大时间点
		if (loginTime > maxPoint || (loginTime + onlineTime) < startPoint) { // 登陆时间大于最大时间或小于最小时间,直接跳过
			return;
		}

		mapKeyObj.reset();
		
		if (loginTime > 0 && onlineTime > 0) {
			for (int point = (loginTime < startPoint ? startPoint : loginTime); point < maxPoint; point++) {
				if (loginTime <= point && onlineTime >= (point - loginTime)) { // 登陆时间在point点之前且在线时长大于(point点-登陆时间)记为一次在线
					
					String[] keyFields = new String[] { 
							appId, 
							platform, 
							gameServer, 
							"" + point,
							Constants.DATA_FLAG_PCU
					};
					mapKeyObj.setOutFields(keyFields);
					context.write(mapKeyObj, one);
					
					/*//分区服统计
					if(!StringUtil.isEmpty(gameServer) && !MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(gameServer.trim())){
						String[] keyFields = new String[] { appId, 
															platform, 
															gameServer, 
															"" + point,
															Constants.DATA_FLAG_PCU};
						mapKeyObj.setOutFields(keyFields);
						context.write(mapKeyObj, one);
					}
					
					// 不分区服的统计，gameServer 以 '-' 代替
					String[] keyFieldsAll = new String[] {appId, 
														  platform, 
														  MRConstants.INVALID_PLACE_HOLDER_CHAR,
														  "" + point,
														  Constants.DATA_FLAG_PCU};
					mapKeyObj.setOutFields(keyFieldsAll);
					context.write(mapKeyObj, one);*/
					
				}
				
				// 可以跳过了
				if (loginTime <= point && onlineTime <= (point - loginTime)) {
					break;
				}
			}
		}
	}

	private void processOnlineCount(String accountId, String appId, String platform, String gameServer, 
									Context context) throws IOException, InterruptedException{
		mapKeyObj.reset();
		String[] keyFields = new String[] { 
				appId, 
				platform, 
				gameServer, 
				accountId, 
				Constants.DATA_FLAG_CNT
		};
		mapKeyObj.setOutFields(keyFields);
		context.write(mapKeyObj, one);

		/*//分区服统计
		if(!StringUtil.isEmpty(gameServer) && !MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(gameServer.trim())){
			String[] keyFields = new String[] { appId, 
												platform, 
												gameServer, 
												accountId, 
												Constants.DATA_FLAG_CNT};
			mapKeyObj.setOutFields(keyFields);
			context.write(mapKeyObj, one);
		}
		
		// 不分区服的统计，gameServer 以 '-' 代替
		String[] keyFieldsAll = new String[] {appId, 
											  platform, 
											  MRConstants.INVALID_PLACE_HOLDER_CHAR, 
											  accountId, 
											  Constants.DATA_FLAG_CNT};
		mapKeyObj.setOutFields(keyFieldsAll);
		context.write(mapKeyObj, one);*/
	}
	
	private int getTimePointIndex(int loginTime){
		int result = Arrays.binarySearch(timePointArr, loginTime);
		int index = Math.abs(result < 0 ? result + 1 : result);
		return index;
	}
}
