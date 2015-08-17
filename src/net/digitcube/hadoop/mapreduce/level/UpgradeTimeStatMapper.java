package net.digitcube.hadoop.mapreduce.level;

import java.io.IOException;
import java.util.Map;

import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author rickpan
 * @version 1.0 
 * 
 * 主要逻辑：
 * 根据自定义事件 MR 中根据 _DESelf_Level 事件 id 抽取出来的升级事件日志
 * 统计每一级升级所用的平均时长及时长区间分布
 * 
 * 输入：
 * 自定义事件  MR 中根据 _DESelf_Level 事件 id 抽取出来的升级事件日志(@EventStatisticsMapper)
 * appID,platform,channel,gameServer,eventID, duration(s, 单位秒)
 * 
 * Map
 * Key:	appId, platform, channel, gameServer, Constants.DIMENSION_LEVEL_UPGRADE_TIME, level
 * Val: duration
 * 
 * Reduce：
 * 升级平均时长：sum(duration)/count(duration)
 * 
 * 时长区间统计：
 * Map:
 * key: appId, platform, channel, gameServer, Constants.DIMENSION_LEVEL_TIME_DIST, level, timeRange
 * val:1
 * 
 * Reduce:
 * 人数：sum(1)

 * 输出
 * 升级平均时长 ：appId, platform, channel, gameServer, playerType, leut(level up time), level, sum(duration)/count(duration)
 * 升级时长区间分布人数：appId, platform, channel, gameServer, playerType, letd(level time distribute), level, timeRange, sum(1)
 * 
 * 
 */
public class UpgradeTimeStatMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private IntWritable mapValObj = new IntWritable();
	private static final int invalidLevelInter = 100;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = new EventLog(array);
		String appID = eventLog.getAppID();
		String platform = eventLog.getPlatform();
		String channel = eventLog.getChannel();
		String gameServer = eventLog.getGameServer();
		String eventID = eventLog.getEventId();
		int duration = eventLog.getDuration();//单位为秒
		
		Map<String, String> attrMap = eventLog.getArrtMap();
		
		// 20141216 : 升级时长改为优先使用实际在线时长
		// duration 为自然时长，spendTimeInLevel 为实际在线时长
		String spendTimeInLevel = attrMap.get("spendTimeInLevel");
		if(null != spendTimeInLevel){
			int realDuration = StringUtil.convertInt(spendTimeInLevel, 0);
			if(realDuration > 0){
				duration = realDuration; 
			}
		}
		
		//Added at 20140825: duration 小于 0 的属于无效数据，丢弃
		if(duration < 0){
			return;
		}
		
		
		//升级日志前后有两个版本
		//版本1日志格式为：commonHeader _DESelf_Level:3(level) 50(duration) -
		//版本2日志格式为：commonHeader _DESelf_Level 		 	50(duration) startLevel:1,endLevel:2 
		if(Constants.EVENT_ID_LEVEL_UP.equals(eventID)){//版本2 _DESelf_Level
			
			int startLevel = StringUtil.convertInt(attrMap.get("startLevel"),0);
			int endLevel = StringUtil.convertInt(attrMap.get("endLevel"),0);
			
			//无效等级过滤掉
			if(0 == endLevel || endLevel <=startLevel || Math.abs(endLevel - startLevel) > invalidLevelInter){
				return;
			}
			
			duration = duration/(endLevel - startLevel);
			
			while(startLevel < endLevel){
				startLevel++;
				String level = "" + startLevel;
			
				//升级时长统计
				//真实区服
				String[] upgradeTime = new String[]{appID,
													platform,
													channel,
													gameServer,
													Constants.PLAYER_TYPE_ONLINE,
													Constants.DIMENSION_LEVEL_UPGRADE_TIME,
													level};
				//全服
				String[] upgradeTime_AllGS = new String[]{
						appID,
						platform,
						channel,
						MRConstants.ALL_GAMESERVER,
						Constants.PLAYER_TYPE_ONLINE,
						Constants.DIMENSION_LEVEL_UPGRADE_TIME,
						level
				};
				mapKeyObj.setOutFields(upgradeTime);
				mapValObj.set(duration);
				mapKeyObj.setSuffix(Constants.SUFFIX_LEVEL_UP_TIME_STAT);
				context.write(mapKeyObj, mapValObj);
				//全服
				mapKeyObj.setOutFields(upgradeTime_AllGS);
				context.write(mapKeyObj, mapValObj);
				
				
				//升级时长区间分布统计
				//真实区服
				int timeRange = EnumConstants.getItval4UpgradeTime(duration);
				String[] upgradeTimeRange = new String[]{appID,
														platform,
														channel,
														gameServer,
														Constants.PLAYER_TYPE_ONLINE,
														Constants.DIMENSION_LEVEL_TIME_DIST,
														level,
														""+timeRange};
				//全服
				String[] upgradeTimeRange_AllGS = new String[]{
						appID,
						platform,
						channel,
						MRConstants.ALL_GAMESERVER,
						Constants.PLAYER_TYPE_ONLINE,
						Constants.DIMENSION_LEVEL_TIME_DIST,
						level,
						""+timeRange
				};
				mapKeyObj.setOutFields(upgradeTimeRange);
				mapValObj.set(1);
				mapKeyObj.setSuffix(Constants.SUFFIX_LEVEL_UP_TIMERANGE);
				context.write(mapKeyObj, mapValObj);
				//全服
				mapKeyObj.setOutFields(upgradeTimeRange_AllGS);
				context.write(mapKeyObj, mapValObj);
			}
		}else{ //版本1 _DESelf_Level:3(level)
		
			String[] eventIDLevel = eventID.split(":");
			String level = "0";
			if(eventIDLevel.length>1){
				level = eventIDLevel[1]; 
			}
			// 如果解析得到的等级小于等于 0 ，则丢弃该条数据
			if(StringUtil.convertInt(level, 0) <= 0){
				return;
			}
			
			//升级时长统计
			//真实区服
			String[] upgradeTime = new String[]{appID,
												platform,
												channel,
												gameServer,
												Constants.PLAYER_TYPE_ONLINE,
												Constants.DIMENSION_LEVEL_UPGRADE_TIME,
												level};
			//全服
			String[] upgradeTime_AllGS = new String[]{
					appID,
					platform,
					channel,
					MRConstants.ALL_GAMESERVER,
					Constants.PLAYER_TYPE_ONLINE,
					Constants.DIMENSION_LEVEL_UPGRADE_TIME,
					level
			};
			mapKeyObj.setOutFields(upgradeTime);
			mapValObj.set(duration);
			mapKeyObj.setSuffix(Constants.SUFFIX_LEVEL_UP_TIME_STAT);
			context.write(mapKeyObj, mapValObj);
			//全服
			mapKeyObj.setOutFields(upgradeTime_AllGS);
			context.write(mapKeyObj, mapValObj);
			
			
			//升级时长区间分布统计
			//真实区服
			int timeRange = EnumConstants.getItval4UpgradeTime(duration);
			String[] upgradeTimeRange = new String[]{appID,
													platform,
													channel,
													gameServer,
													Constants.PLAYER_TYPE_ONLINE,
													Constants.DIMENSION_LEVEL_TIME_DIST,
													level,
													""+timeRange};
			//全服
			String[] upgradeTimeRange_AllGS = new String[]{
					appID,
					platform,
					channel,
					MRConstants.ALL_GAMESERVER,
					Constants.PLAYER_TYPE_ONLINE,
					Constants.DIMENSION_LEVEL_TIME_DIST,
					level,
					""+timeRange
			};
			mapKeyObj.setOutFields(upgradeTimeRange);
			mapValObj.set(1);
			mapKeyObj.setSuffix(Constants.SUFFIX_LEVEL_UP_TIMERANGE);
			context.write(mapKeyObj, mapValObj);
			//全服
			mapKeyObj.setOutFields(upgradeTimeRange_AllGS);
			context.write(mapKeyObj, mapValObj);
		}
	}
}
