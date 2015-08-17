package net.digitcube.hadoop.mapreduce.roomtimes;

import java.io.IOException;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.TmpOutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 给 cherry 收取信息用，不作为理性化 MR 运行
 * <pre>
 *  用户房间局数 Map-2
 *  
 *  Title: RoomPlayTimesMapper.java<br>
 *  Description: RoomPlayTimesMapper.java<br>
 *  Copyright: Copyright (c) 2013 www.dataeye.com<br>
 *  Company: DataEye 数据之眼<br>
 *  
 *  @author Ivan     <br>
 *  @date 2013-10-31         <br>
 *  @version 1.0          <br>
 *  
 *  
 *  
 *  其依赖的MR 输出是
 *  [ appID, platform, accountID,channel,gameserver,accountType, gender, age, province,roomid,sum(new),sum(active),sum(pay)] suffix:ROOM_DATA
 *  
 *  该Map 输出：
 *  key:
 * [appID,platform,channel,gameserver,roomid]
 *  value:
 * [sum(new),sum(active),sum(pay)]
 * 
 */
public class RoomPlayTimesForCherryMapper extends Mapper<LongWritable, Text, TmpOutFieldsBaseModel, IntWritable> {
	private TmpOutFieldsBaseModel keyObj = new TmpOutFieldsBaseModel();
	private IntWritable valObj = new IntWritable();
			
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String[] keyFields = null;

		//appID, platform, accountID,channel,gameserver,accountType, gender, age, province,roomid,sum(new),sum(active),sum(pay)
		String[] appInfo = paraArr[0].split("\\|");
		if(!paraArr[0].contains("4CD596BDFB0E2D247CB45C8F3072D31D") || appInfo.length < 2){
			return;
		}
		String appID = appInfo[0];
		//String appVersion = appInfo[1];
		String platform = paraArr[1];
		String channel = paraArr[3];
		String gameserver = paraArr[4];
		String roomid = paraArr[9];
		int newPlayTimes = StringUtil.convertInt(paraArr[10],0);
		int oldPlayTimes = StringUtil.convertInt(paraArr[11],0);
		int payPlayTimes = StringUtil.convertInt(paraArr[12],0);
		
		if(!"2".equals(platform) || !MRConstants.ALL_GAMESERVER.equals(gameserver)){
			return;
		}
		
		//房间人数------------------------------------------
		keyObj.setSuffix("RoomPlayerNum");
		//新增玩家人数
		if(newPlayTimes > 0){
			keyFields = new String[] {appID, platform, roomid, DimenType.NEW_PLAYER_NUM.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//老玩家人数（活跃非新增玩家）
		if(newPlayTimes <= 0 && oldPlayTimes > 0){
			keyFields = new String[] {appID, platform, roomid, DimenType.OLD_PLAYER_NUM.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//付费玩家
		if(payPlayTimes > 0){
			keyFields = new String[] {appID, platform, roomid, DimenType.PAY_PLAYER_NUM.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//非付费玩家
		else{
			keyFields = new String[] {appID, platform, roomid, DimenType.NPAY_PLAYER_NUM.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		
		//房间总局数------------------------------------------
		keyObj.setSuffix("RoomTotalRounds");
		//新增玩家局数
		if(newPlayTimes > 0){
			keyFields = new String[] {appID, platform, roomid, DimenType.NEW_TOTAL_ROUNDS.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(newPlayTimes);
			context.write(keyObj, valObj);
			
		}
		//老玩家局数（活跃非新增玩家）
		if(newPlayTimes <= 0 && oldPlayTimes > 0){
			keyFields = new String[] {appID, platform, roomid, DimenType.OLD_TOTAL_ROUNDS.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(oldPlayTimes);
			context.write(keyObj, valObj);
		}
		//付费玩家局数
		if(payPlayTimes > 0){
			keyFields = new String[] {appID, platform, roomid, DimenType.PAY_TOTAL_ROUNDS.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(payPlayTimes);
			context.write(keyObj, valObj);
		}
		//非付费玩家局数
		else{
			keyFields = new String[] {appID, platform, roomid, DimenType.NPAY_TOTAL_ROUNDS.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(oldPlayTimes);//活跃玩家的次数即可
			context.write(keyObj, valObj);
		}
		
		//人数统计(即 1 局以上)------------------------------------------
		keyObj.setSuffix("RoomPlayRounds01");
		//新增玩家局数
		if(newPlayTimes > 0){
			keyFields = new String[] {appID, platform, roomid, DimenType.NEW_PLAY_ROUNDS.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//老玩家局数（活跃非新增玩家）
		if(newPlayTimes <= 0 && oldPlayTimes > 0){
			keyFields = new String[] {appID, platform, roomid, DimenType.OLD_PLAY_ROUNDS.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//付费玩家局数
		if(payPlayTimes > 0){
			keyFields = new String[] {appID, platform, roomid, DimenType.PAY_PLAY_ROUNDS.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//非付费玩家局数
		else{
			keyFields = new String[] {appID, platform, roomid, DimenType.NPAY_PLAY_ROUNDS.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);//活跃玩家的次数即可
			context.write(keyObj, valObj);
		}
		
		//人数统计(3 局以上)------------------------------------------
		keyObj.setSuffix("RoomPlayRounds03");
		//新增玩家人数
		if(newPlayTimes >= 3){
			keyFields = new String[] {appID, platform, roomid, DimenType.NEW_PLAY_ROUNDS3.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//老玩家人数（活跃非新增玩家）
		if(newPlayTimes <= 0 && oldPlayTimes >= 3){
			keyFields = new String[] {appID, platform, roomid, DimenType.OLD_PLAY_ROUNDS3.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//付费玩家人数
		if(payPlayTimes >= 3){
			keyFields = new String[] {appID, platform, roomid, DimenType.PAY_PLAY_ROUNDS3.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//非付费玩家人数
		if(payPlayTimes <= 3 && oldPlayTimes >= 3){
			keyFields = new String[] {appID, platform, roomid, DimenType.NPAY_PLAY_ROUNDS3.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		
		//人数统计(7 局以上)------------------------------------------
		keyObj.setSuffix("RoomPlayRounds07");
		//新增玩家人数
		if(newPlayTimes >= 7){
			keyFields = new String[] {appID, platform, roomid, DimenType.NEW_PLAY_ROUNDS7.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//老玩家人数（活跃非新增玩家）
		if(newPlayTimes <= 0 && oldPlayTimes >= 7){
			keyFields = new String[] {appID, platform, roomid, DimenType.OLD_PLAY_ROUNDS7.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//付费玩家人数
		if(payPlayTimes >= 7){
			keyFields = new String[] {appID, platform, roomid, DimenType.PAY_PLAY_ROUNDS7.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//非付费玩家人数
		if(payPlayTimes <= 7 && oldPlayTimes >= 7){
			keyFields = new String[] {appID, platform, roomid, DimenType.NPAY_PLAY_ROUNDS7.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		
		//人数统计(15 局以上)------------------------------------------
		keyObj.setSuffix("RoomPlayRounds15");
		//新增玩家人数
		if(newPlayTimes >= 15){
			keyFields = new String[] {appID, platform, roomid, DimenType.NEW_PLAY_ROUNDS15.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//老玩家人数（活跃非新增玩家）
		if(newPlayTimes <= 0 && oldPlayTimes >= 15){
			keyFields = new String[] {appID, platform, roomid, DimenType.OLD_PLAY_ROUNDS15.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//付费玩家人数
		if(payPlayTimes >= 15){
			keyFields = new String[] {appID, platform, roomid, DimenType.PAY_PLAY_ROUNDS15.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//非付费玩家人数
		if(payPlayTimes <= 15 && oldPlayTimes >= 15){
			keyFields = new String[] {appID, platform, roomid, DimenType.NPAY_PLAY_ROUNDS15.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		
		//人数统计(30 局以上)------------------------------------------
		keyObj.setSuffix("RoomPlayRounds30");
		//新增玩家人数
		if(newPlayTimes >= 30){
			keyFields = new String[] {appID, platform, roomid, DimenType.NEW_PLAY_ROUNDS30.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//老玩家人数（活跃非新增玩家）
		if(newPlayTimes <= 0 && oldPlayTimes >= 30){
			keyFields = new String[] {appID, platform, roomid, DimenType.OLD_PLAY_ROUNDS30.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//付费玩家人数
		if(payPlayTimes >= 30){
			keyFields = new String[] {appID, platform, roomid, DimenType.PAY_PLAY_ROUNDS30.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
		//非付费玩家人数
		if(payPlayTimes <= 30 && oldPlayTimes >= 30){
			keyFields = new String[] {appID, platform, roomid, DimenType.NPAY_PLAY_ROUNDS30.getType()};
			keyObj.setOutFields(keyFields);
			valObj.set(1);
			context.write(keyObj, valObj);
		}
	}
	
	public enum DimenType{
		//房间人数
		NEW_PLAYER_NUM("NewPlayerNum"),
		OLD_PLAYER_NUM("OldPlayerNum"),
		PAY_PLAYER_NUM("PayPlayerNum"),
		NPAY_PLAYER_NUM("NPayPlayerNum"),
		//房间总局数
		NEW_TOTAL_ROUNDS("NewTotalRounds"),
		OLD_TOTAL_ROUNDS("OldTotalRounds"),
		PAY_TOTAL_ROUNDS("PayTotalRounds"),
		NPAY_TOTAL_ROUNDS("NPayTotalRounds"),
		//1 局以上人数
		NEW_PLAY_ROUNDS("NewPlayRounds01"),
		OLD_PLAY_ROUNDS("OldPlayRounds01"),
		PAY_PLAY_ROUNDS("PayPlayRounds01"),
		NPAY_PLAY_ROUNDS("NPayPlayRounds01"),
		//3 局以上人数
		NEW_PLAY_ROUNDS3("NewPlayRounds03"),
		OLD_PLAY_ROUNDS3("OldPlayRounds03"),
		PAY_PLAY_ROUNDS3("PayPlayRounds03"),
		NPAY_PLAY_ROUNDS3("NPayPlayRounds03"),
		//7 局以上人数
		NEW_PLAY_ROUNDS7("NewPlayRounds07"),
		OLD_PLAY_ROUNDS7("OldPlayRounds07"),
		PAY_PLAY_ROUNDS7("PayPlayRounds07"),
		NPAY_PLAY_ROUNDS7("NPayPlayRounds07"),
		//15 局以上人数
		NEW_PLAY_ROUNDS15("NewPlayRounds15"),
		OLD_PLAY_ROUNDS15("OldPlayRounds15"),
		PAY_PLAY_ROUNDS15("PayPlayRounds15"),
		NPAY_PLAY_ROUNDS15("NPayPlayRounds15"),
		//30 局以上人数
		NEW_PLAY_ROUNDS30("NewPlayRounds30"),
		OLD_PLAY_ROUNDS30("OldPlayRounds30"),
		PAY_PLAY_ROUNDS30("PayPlayRounds30"),
		NPAY_PLAY_ROUNDS30("NPayPlayRounds30");
		private String type;
		private DimenType(String type){
			this.type = type;
		}
		public String getType(){
			return this.type;
		}
	}
}
