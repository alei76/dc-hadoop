package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑：
 * 
 * 输入：
 * a) 滚存中输出的玩家在线信息(@UserInfoRollingDayMapper --> Constants.SUFFIX_PLAYER_ONLINE_INFO)
 * 该输入包括新增活跃付费玩家的信息，分别在：
 * 1) 设备（机型、分辨率、运营商、操作系统、和联网方式） 
 * 2) 玩家属性（地区、性别、帐号类型、年龄）进行分布统计
 * 3) 同时又对这些玩家的单次游戏时长和游戏时段进行统计
 * 
 * b) 当天滚存的流失玩家信息(@UserInfoRollingDayMapper --> Constants.SUFFIX_USERFLOW)
 * c) 每台设备帐号数去重后结果(@AccountNumPerDeviceMapper)
 * d) 等级滞停的输出结果(@LevelStopMapper)
 *  
 * 其中
 * 当天滚存的流失玩家信息用于统计各个等级中 7/14/30 流失玩家的分布情况
 * 每台设备帐号数去重后结果用于统计单台设备中帐号数量的分布情况
 * 等级滞停用于统计各个等级 3 天内玩家等级没有升级的玩家数量分布情况

 * 
 * Map：
 * key : appId, platId, channel, gameServer, playerType, 维度指标(如联网方式, netType), 维度值(3G)
 * val : 1
 * 
 * Reduce :
 * appId, platId, channel, gameServer, playerType, 维度指标(如联网方式, netType), 维度值(3G), sum(1)
 * 
 */

public class SiShenLoginIntervalMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private String fileName = "";
	
	private final IntWritable levelStat = new IntWritable();
	private Calendar cal = Calendar.getInstance();
	
	//添加是否小时任务 add by mikefeng 20141013
	private boolean isHour = false;
	private int currentHour = 23;
	
	private Map<Integer, String> intervalMap = new HashMap<Integer, String>();
	
	public static int[] LoginTimeInterval2 = { 
		120,
		300,
		600,
		1800,
		3600,
		Integer.MAX_VALUE // 30 天以上
	};
	public static int getRangeTop(int val, int[] rangeArr) {
		int result = Arrays.binarySearch(rangeArr, val);
		int index = Math.abs(result < 0 ? result + 1 : result);
		return rangeArr[index];
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		int i = 0;
		intervalMap.put(120, "1~120");
		intervalMap.put(300, "121~300");
		intervalMap.put(600, "301~600");
		intervalMap.put(1800, "601~1800");
		intervalMap.put(3600, "1801~3600");
		intervalMap.put(Integer.MAX_VALUE, "3600+");
	}

	private static final String targetAppId = "EA7D0701220EF0E4ACA26BC41B7C2AF9";
	private static final String targetPlatF = MRConstants.PLATFORM_iOS_STR;
	private static final String targetCh1 = "TBT";
	private static final String targetCh2 = "91YX";
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		
		String playerType = array[array.length - 1];
		String[] onlineDayArr = new String[array.length - 1];
		System.arraycopy(array, 0, onlineDayArr, 0, array.length - 1);
		OnlineDayLog onlineDayLog = new OnlineDayLog(onlineDayArr);
		
		String pureAppId = onlineDayLog.getAppID().split("\\|")[0];
		String platform = onlineDayLog.getPlatform();
		String channel = onlineDayLog.getExtend().getChannel().trim().toUpperCase();
		/*if(pureAppId.equals(targetAppId)){
			System.out.println("targetAppId-->"+pureAppId+"\t"
								+platform+"\t"
								+onlineDayLog.getExtend().getGameServer()+"\t"
								+channel);
		}*/
		if(pureAppId.equals(targetAppId) && targetPlatF.equals(platform) 
				&& MRConstants.ALL_GAMESERVER.equals(onlineDayLog.getExtend().getGameServer().trim())
				&& (targetCh1.equals(channel) || targetCh2.equals(channel))){
			writeLoginTimeInfo(context, onlineDayLog, "O");
			}
	}
	
	/**
	 * a) 统计玩家每次登录所属时段
	 * b) 统计玩家两次登录的时间间隔
	 * 
	 * @param context
	 * @param onlineDayLog
	 * @param playerType
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void writeLoginTimeInfo(Context context, OnlineDayLog onlineDayLog, String playerType) throws IOException, InterruptedException{
		
		String[] onlineRecords = onlineDayLog.getOnlineRecords().split(",");
		
		//设置后缀为按玩家属性分布
		mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);
		//登录时长间隔
		TreeSet<Integer> loginTimeSet = new TreeSet<Integer>();
		// Added at 20140731
		// 注意：onlineDayLog.getLastLoginTime 字段
		// a) 在 OnlineDay MR 输出时，该字段保存的是当天的最后登录时间
		// b) 在滚出输出时，该字段保存的是出当天外历史中最后一次最后登录时间
		// 所以这里为了统计两次登录时间间隔时，必须把历史中最后一次最后登录时间加上
		loginTimeSet.add(onlineDayLog.getLastLoginTime());
		
		//取出玩家当天的所有登录信息，并输出单次游戏时长统计
		for(String onlineRecord : onlineRecords){
			String[] record = onlineRecord.split(":");
			if(record.length < 2){
				continue;
			}
			
			int loginTime = StringUtil.convertInt(record[0], 0);
			//不处理无效的数据
			if(loginTime <= 0){
				continue;
			}
			
			//记录登录时间，用户统计登录间隔
			loginTimeSet.add(loginTime);
		}
		
		//登录间隔时长分布
		int lastLoginTime = 0;
		for(Integer loginTime : loginTimeSet){
			if(0 == lastLoginTime){
				lastLoginTime = loginTime;
			}else{
				int loginTimeInterval = loginTime - lastLoginTime;
				//loginTimeInterval = EnumConstants.getRangeTop4LoginTime2(loginTimeInterval);
				loginTimeInterval = getRangeTop(loginTimeInterval, LoginTimeInterval2);
				String[] loginInterval = new String[]{
						onlineDayLog.getAppID().split("\\|")[0], 
						onlineDayLog.getExtend().getChannel(),
						intervalMap.get(loginTimeInterval)
				};
				mapKeyObj.setOutFields(loginInterval);
				context.write(mapKeyObj, one);
				
				// 
				lastLoginTime = loginTime;
			}
		}
	}
}
