package net.digitcube.hadoop.mapreduce.roomtimes;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import net.digitcube.hadoop.common.AttrUtil;
import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.jce.PlayerDayInfo;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <pre>
 *  房间局数统计 MR-1
 *  
 *  Title: RoomAndUserRollingMapper.java<br>
 *  Description: RoomAndUserRollingMapper.java<br>
 *  Copyright: Copyright (c) 2013 www.dataeye.com<br>
 *  Company: DataEye 数据之眼<br>
 *  
 *  @author Ivan     <br>
 *  @date 2013-10-31         <br>
 *  @version 1.0
 *  <br>
 *  
 *  输入文件：1.滚存日志
 *          2.用户在某个房间完了一局的日志（_DESelf_CG_PlayCards）
 * 如果是滚存日志：         
 *  Map:
 *       key = [ appID, platform, accountID ]
 *       value = [ 当前用户的所有类型身份] suffix:USER_TYPE
 *  如果是房间局数日志
 *  	 key = [ appID, platform, accountID ]
 * value=  [channel, gameServer, roomid, accountType, gender, age, province] suffix:ROOM_DATA
 * 
 * 
 */
public class RoomAndUserRollingMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private Calendar calendar = null;
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	//加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		calendar = Calendar.getInstance();
		Date date = ConfigManager.getInitialDate(context.getConfiguration());
		if (date != null) {
			calendar.setTime(date);
		}
		calendar.add(Calendar.DATE, -1);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();

		String[] keyFields = null;
		String[] valueFields = null;

		if (fileSuffix.contains(Constants.SUFFIX_USERROLLING)) { // 用户滚存日志
			//UserInfoRollingLog userInfoRollingLog = new UserInfoRollingLog(paraArr);
			UserInfoRollingLog userInfoRollingLog = new UserInfoRollingLog(scheduleTime, paraArr);
			String appID = userInfoRollingLog.getAppID();
			String platform = userInfoRollingLog.getPlatform();
			String accountID = userInfoRollingLog.getAccountID();

			PlayerDayInfo playerDayInfo = userInfoRollingLog.getPlayerDayInfo();
			int firstLoginDate = playerDayInfo.getFirstLoginDate();
			int totalCurrencyAmount = playerDayInfo.getTotalCurrencyAmount();
			int totalPayTimes = playerDayInfo.getTotalPayTimes();

			Set<String> userType = new HashSet<String>(3);
			userType.add(Constants.PLAYER_TYPE_ONLINE);
			if (firstLoginDate == calendar.getTimeInMillis() / 1000) {
				// 新增用户
				userType.add(Constants.PLAYER_TYPE_NEWADD);
			}
			if (totalCurrencyAmount > 0 || totalPayTimes > 0) {
				// 付费用户
				userType.add(Constants.PLAYER_TYPE_PAYMENT);
			}
			keyFields = new String[] { appID, platform, accountID, userInfoRollingLog.getPlayerDayInfo().getGameRegion()};
			valueFields = userType.toArray(new String[userType.size()]);

			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valueFields);
			valObj.setSuffix(Constants.SUFFIX_USER_TYPE);
			context.write(keyObj, valObj);
		} else if (fileSuffix.contains(Constants.DESelf_CG_PlayCards)) {
			EventLog eventLog = new EventLog(paraArr);
			String appID = eventLog.getAppID();
			String accountID = eventLog.getAccountID();
			String platform = eventLog.getPlatform();

			String channel = eventLog.getChannel();
			String accountType = eventLog.getAccountType();
			String gender = eventLog.getGender();
			String age = eventLog.getAge();
			String gameServer = eventLog.getGameServer();
			String province = eventLog.getProvince();

			//String eventAttr = paraArr[paraArr.length - 1];

			// 取属性 roomId
			//String roomid = AttrUtil.getEventAttrValue(eventAttr, "roomId");
			String roomid = eventLog.getArrtMap().get("roomId");
			// 去除roomid为空的记录
			if(StringUtil.isEmpty(roomid)){
				return;
			}			
			//真实区服
			keyFields = new String[] { appID, platform, accountID, gameServer};
			//全服
			String[] keyFields_AllGS = new String[] { appID, platform, accountID, MRConstants.ALL_GAMESERVER};
			valueFields = new String[] { channel, gameServer, accountType, gender, age, province, roomid };

			keyObj.setOutFields(keyFields);
			valObj.setSuffix(Constants.SUFFIX_ROOM_DATA);
			valObj.setOutFields(valueFields);
			context.write(keyObj, valObj);
			//全服
			keyObj.setOutFields(keyFields_AllGS);
			valueFields[1] = MRConstants.ALL_GAMESERVER;
			valObj.setOutFields(valueFields);
			context.write(keyObj, valObj);
		}

	}
}

