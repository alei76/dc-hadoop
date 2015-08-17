package net.digitcube.hadoop.mapreduce.cards;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 输入: 前 30 天玩家每天的局数汇总结果(see @RoomAndUserRollingMapper)
 * [appID, platform, accountID, channel, gameserver, accountType, gender, age, province,
 *  roomid, sum(new), sum(active), sum(pay)] suffix:ROOM_DATA
 * 
 * Reduce 中判断：
 * a) 1/7/30 日有效玩家
 * 1 日有效玩家   ：统计当日累计进行游戏局数≥3局的玩家
 * 7 日有效玩家   ：统计当日往前推7日的时间期间内，至少登陆游戏2天（≥2）并累计进行游戏6局（≥6）以上的玩家
 * 30 日有效玩家：统计当日往前推30日的时间内，累计登录游戏至少5（≥5）天，并进行局数至少15（≥15）局的玩家
 * 
 * b) 是否新增玩家
 * 只要其中某天的  sum(new) > 0 即为新增玩家( 只有在新增当天 sum(new) 才会大于 0 )
 * 
 * Map:
 * key = appid, platform, accountId, DATE_FLAG(v1,v7,v30)
 * val = DATA_FLAG(newAddPlayer/playCards), dateOfInputDir, channel, gameserver, area, age, gender, accountType
 * val = dateOfInputDir, newPlayRounds, actPlayRounds, channel, gameserver, area, age, gender, accountType
 * 
 * 
 * Reduce:
 * appid, platform, gameserver, accountId, DATE_FLAG(v1,v7,v30), playerFlag(1-->newAdd), channel, accountType, province, age, gender
 * 
 */
public class ValidPlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	//static final String DATA_FLAG_NEW_ADD = "NEW";
	//static final String DATA_FLAG_PLAY_CARD = "CARD";
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	private String fileName = "";
	private int dateOfInputDir = 0;
	private int statDate_1 = 0;
	private int statDate_7 = 0;
	private int statDate_30 = 0;
	//private boolean statWeek = false;
	//private boolean statMonth = false;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Path inputPath = ((FileSplit)context.getInputSplit()).getPath();
		fileName = inputPath.getName();
		
		//在输入路径中，从根目录开始，年份这个目录排在第几层
		//如 /data/digitcube/online_hour/2013/10/27/23/output 中年份排在第四
		//所以 year.index.in.path 的值应该设置为 4
		int yearIndex = context.getConfiguration().getInt("year.index.in.path", 4);
		String input = inputPath.toString();
		
		// 输入可能包含 'hdfs://'，将其去掉下面通过 '/' 分割是 year.index 才能正确
		input = input.replace("://",":"); 
		String[] components = input.split("/");
		
		//inputDirDate = yyyyMMdd
		String yyyyMMdd = components[yearIndex]	//year
						 + components[yearIndex+1] //month
						 + components[yearIndex+2];//day
		dateOfInputDir = Integer.valueOf(yyyyMMdd);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		Date date = ConfigManager.getInitialDate(context.getConfiguration());
		if (date != null) {
			cal.setTime(date);
		}
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		
		/*// 如果是周一,则统计有效玩家在自然月里的分布
		if(Calendar.MONDAY == cal.get(Calendar.DAY_OF_WEEK)){
			statWeek = true;
		}
				
		// 如果是 1 号,则统计有效玩家在自然月里的分布
		if(1 == cal.get(Calendar.DAY_OF_MONTH)){
			statMonth = true;
		}*/
			
		// statDate_1
		cal.add(Calendar.DAY_OF_MONTH, -1);
		yyyyMMdd = sdf.format(cal.getTime());
		statDate_1 = Integer.valueOf(yyyyMMdd);
		
		// statDate_7
		cal.add(Calendar.DAY_OF_MONTH, -6);
		yyyyMMdd = sdf.format(cal.getTime());
		statDate_7 = Integer.valueOf(yyyyMMdd);
		
		// statDate_30
		cal.add(Calendar.DAY_OF_MONTH, -23);
		yyyyMMdd = sdf.format(cal.getTime());
		statDate_30 = Integer.valueOf(yyyyMMdd);
	}


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] inputArr = value.toString().split(MRConstants.SEPERATOR_IN);
		/*String[] keyFields = null;
		String[] valFields = null;

		String appId = null; 
		String accountId = null;
		String platform = null;
		String gameServer = null;
		if(fileName.endsWith(Constants.LOG_FLAG_ONLINE_FIRST)){
			appId = inputArr[Constants.INDEX_APPID];
			platform = inputArr[Constants.INDEX_PLATFORM];
			accountId = inputArr[Constants.INDEX_ACCOUNTID];
			gameServer = inputArr[Constants.INDEX_GAMESERVER];
			valFields = new String[]{DATA_FLAG_NEW_ADD};
		}else if(fileName.endsWith(Constants.SUFFIX_ROOM_DATA)){
			
			appId = inputArr[0];
			platform = inputArr[1];
			accountId = inputArr[2];
			gameServer = inputArr[4];
			
			String channel = inputArr[3]; 
			String accountType = inputArr[5];
			String gender = inputArr[6];
			String age = inputArr[7];
			String province = inputArr[8];
			String newPlayRounds = inputArr[10];
			String actPlayRounds = inputArr[11];
			valFields = new String[]{DATA_FLAG_PLAY_CARD, ""+dateOfInputDir, actPlayRounds, channel, accountType, province, age, gender};
		}else{
			return;
		}*/
		
		if(fileName.endsWith(Constants.SUFFIX_ROOM_DATA)){
			String appId = inputArr[0];
			String platform = inputArr[1];
			String accountId = inputArr[2];
			String gameServer = inputArr[4];
			
			String channel = inputArr[3]; 
			String accountType = inputArr[5];
			String gender = inputArr[6];
			String age = inputArr[7];
			String province = inputArr[8];
			String newPlayRounds = inputArr[10];
			String actPlayRounds = inputArr[11];
			
			//valFields = new String[]{DATA_FLAG_PLAY_CARD, ""+dateOfInputDir, actPlayRounds, channel, accountType, province, age, gender};
			String[] valFields = new String[]{""+dateOfInputDir, newPlayRounds, actPlayRounds, channel, accountType, province, age, gender};
			
			if(dateOfInputDir < statDate_7){ // 7 日以前的数据，进行 30 有效统计范畴
				String[] keyFields = new String[]{appId, platform, gameServer, accountId, Constants.PLAYER_VALID_30};
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
				
			}else if(dateOfInputDir < statDate_1){ // 1 日以前的数据，进行 7/30 日有效统计
				// 7 日有效统计
				String[] keyFields = new String[]{appId, platform, gameServer, accountId, Constants.PLAYER_VALID_7};
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
				
				// 30 日有效统计
				keyFields = new String[]{appId, platform, gameServer, accountId, Constants.PLAYER_VALID_30};
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
				
			}else{ // 当天的数据，进行 1/7/30 日有效统计
				
				// 1 日有效统计
				String[] keyFields = new String[]{appId, platform, gameServer, accountId, Constants.PLAYER_VALID_1};
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
				
				// 7 日有效统计
				keyFields = new String[]{appId, platform, gameServer, accountId, Constants.PLAYER_VALID_7};
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
				
				// 30 日有效统计
				keyFields = new String[]{appId, platform, gameServer, accountId, Constants.PLAYER_VALID_30};
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
			}
		}
	}
}
