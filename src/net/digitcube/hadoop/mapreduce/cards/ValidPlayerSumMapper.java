package net.digitcube.hadoop.mapreduce.cards;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author rickpan
 * @version 1.0 
 * <br>
 * 输入：
 * 1/7/30 日有效玩家(@ValidPalyerMapper)
 * 
 * appid, platform, accountId, DATE_FLAG(v1,v7,v30), playerFlag(1-->newAdd), channel, gameserver, accountType, province, age, gender
 * 
 * Map:
 * key(有效玩家统计):
 * appid, platform, channel, gameserver, playerType, 'vp'(有效玩家类型), [v1,v7,v30](1/7/30 有效玩家)
 * 
 * key(分布统计):
 * appid, platform, channel, gameserver, playerType, [v1,v7,v30](dimType 1/7/30 有效玩家), 
 * dimVkey1[accountType,area,age,gender],dimVkey2[accountType,area,age,gender 实际值]
 * 
 * val: 1
 * 
 * Reduce:
 * 有效玩家统计
 * appid, platform, channel, gameserver, playerType, 'vp'(有效玩家类型), [v1,v7,v30](1/7/30 有效玩家), sum(1)
 * 
 * 分布统计:
 * appid, platform, channel, gameserver, playerType, [v1,v7,v30](dimType 1/7/30 有效玩家), dimVkey1,dimVkey2, sum(1)
 */

public class ValidPlayerSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private IntWritable one = new IntWritable(1);
	private boolean statWeek = false;
	private boolean statMonth = false;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		Calendar cal = Calendar.getInstance();
		Date date = ConfigManager.getInitialDate(context.getConfiguration());
		if (date != null) {
			cal.setTime(date);
		}
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		
		// 如果是周一,则统计有效玩家在自然月里的分布
		if(Calendar.MONDAY == cal.get(Calendar.DAY_OF_WEEK)){
			statWeek = true;
		}
				
		// 如果是 1 号,则统计有效玩家在自然月里的分布
		if(1 == cal.get(Calendar.DAY_OF_MONTH)){
			statMonth = true;
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		//appid, platform, gameserver, accountId, DATE_FLAG(v1,v7,v30), playerFlag(1-->newAdd), channel, accountType, province, age, gender
		String appId = arr[0];
		String platform = arr[1];
		String gameServer = arr[2];
		String accountId = arr[3];
		String validFlag = arr[4]; //v1/v7/v30
		
		boolean isNewAdd = "1".equals(arr[5]);
		
		String channel = arr[6];
		String accountType = arr[7];
		String province = arr[8];
		String age = arr[9];
		String gender = arr[10];
		
		// 1/7/30 有效玩家统计
		//不管是否是新增玩家，都视为活跃玩家
		mapKeyObj.setSuffix(Constants.SUFFIX_VALID_PLAYER_SUM);
		String[] keyFields = new String[] {appId, platform, channel, gameServer, Constants.PLAYER_TYPE_ONLINE, Constants.PLAYER_VALID, validFlag};
		mapKeyObj.setOutFields(keyFields);
		context.write(mapKeyObj, one);
		
		//如果当前玩家是新增玩家，则输出新增统计
		if(isNewAdd){
			keyFields[4] = Constants.PLAYER_TYPE_NEWADD;
			mapKeyObj.setOutFields(keyFields);
			context.write(mapKeyObj, one);
		}
		
		//分布统计
		//设置后缀为有效玩家按玩家属性分布
		//通过后缀区分 1/7/30 有效玩家分布
		if(Constants.PLAYER_VALID_1.equals(validFlag)){
			mapKeyObj.setSuffix(Constants.SUFFIX_VALID_P_LAYOUT_DAY);
			//writeValidLayout(context, appId, platform, channel, gameServer, validFlag, isNewAdd, province, gender, age, accountType); 
			writeValidLayout(context, appId, platform, channel, gameServer, isNewAdd, province, gender, age, accountType);
			
		}else if(Constants.PLAYER_VALID_7.equals(validFlag) && statWeek){ // 7 日有效玩家分布则只在每周一计算时才输出统计
			mapKeyObj.setSuffix(Constants.SUFFIX_VALID_P_LAYOUT_WEEK);
			//writeValidLayout(context, appId, platform, channel, gameServer, validFlag, isNewAdd, province, gender, age, accountType); 
			writeValidLayout(context, appId, platform, channel, gameServer, isNewAdd, province, gender, age, accountType);
			
		}else if(Constants.PLAYER_VALID_30.equals(validFlag) && statMonth){ // 30 日有效玩家分布只在每月一号计算时才输出统计
			mapKeyObj.setSuffix(Constants.SUFFIX_VALID_P_LAYOUT_MONTH);
			//writeValidLayout(context, appId, platform, channel, gameServer, validFlag, isNewAdd, province, gender, age, accountType);
			writeValidLayout(context, appId, platform, channel, gameServer, isNewAdd, province, gender, age, accountType);
		}
	}
	
	private void writeValidLayout(Context context, 
								  String appId, String platform, String channel, String gameServer, boolean isNewAdd, 
								  String area, String gender, String age, String accountType ) throws IOException, InterruptedException{
		
		//帐号类型
		String[] accoutTypeArr = new String[]{appId, platform, channel, gameServer, 
												Constants.PLAYER_TYPE_ONLINE, 
												//validFlag,
												Constants.PLAYER_VALID,
							   				  	Constants.DIMENSION_PLAYER_ACCOUNTTYPE, accountType};
				
		//地区
		String[] areaArr = new String[]{appId, platform, channel, gameServer, 
										Constants.PLAYER_TYPE_ONLINE,
										Constants.PLAYER_VALID,
							   			Constants.DIMENSION_PLAYER_AREA, area}; 
		
		//性别
		String[] genderArr = new String[]{appId, platform, channel, gameServer, 
											Constants.PLAYER_TYPE_ONLINE,
											Constants.PLAYER_VALID,
							  			  	Constants.DIMENSION_PLAYER_GENDER, gender}; 
		
		
		//年龄
		int ageNum = StringUtil.convertInt(age, 0);
		//String ageRange = EnumConstants.Age.getAgeRange(ageNum);
		int ageRange = EnumConstants.getRangeTop4Age(ageNum);
		String[] ageArr = new String[]{appId, platform, channel, gameServer, 
										Constants.PLAYER_TYPE_ONLINE, 
										Constants.PLAYER_VALID,
										Constants.DIMENSION_PLAYER_AGE, ""+ageRange}; 
		
		mapKeyObj.setOutFields(accoutTypeArr);
		context.write(mapKeyObj, one);
		
		mapKeyObj.setOutFields(areaArr);
		context.write(mapKeyObj, one);
		
		mapKeyObj.setOutFields(genderArr);
		context.write(mapKeyObj, one);
		
		mapKeyObj.setOutFields(ageArr);
		context.write(mapKeyObj, one);
		
		// 如果是新增玩家,则输出新增玩家分布
		if(isNewAdd){
			accoutTypeArr[4] = Constants.PLAYER_TYPE_NEWADD;
			mapKeyObj.setOutFields(accoutTypeArr);
			context.write(mapKeyObj, one);
			
			areaArr[4] = Constants.PLAYER_TYPE_NEWADD;
			mapKeyObj.setOutFields(areaArr);
			context.write(mapKeyObj, one);
			
			genderArr[4] = Constants.PLAYER_TYPE_NEWADD;
			mapKeyObj.setOutFields(genderArr);
			context.write(mapKeyObj, one);
			
			ageArr[4] = Constants.PLAYER_TYPE_NEWADD;
			mapKeyObj.setOutFields(ageArr);
			context.write(mapKeyObj, one);
		}
		
	}
}
