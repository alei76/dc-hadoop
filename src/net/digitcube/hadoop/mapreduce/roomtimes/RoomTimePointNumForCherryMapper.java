package net.digitcube.hadoop.mapreduce.roomtimes;

import java.io.IOException;
import java.util.Calendar;

import net.digitcube.hadoop.common.AttrUtil;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.TmpOutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.roomtimes.RoomPlayTimesForCherryMapper.DimenType;
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
public class RoomTimePointNumForCherryMapper extends Mapper<LongWritable, Text, TmpOutFieldsBaseModel, TmpOutFieldsBaseModel> {
	private TmpOutFieldsBaseModel keyObj = new TmpOutFieldsBaseModel();
	private TmpOutFieldsBaseModel valObj = new TmpOutFieldsBaseModel();
	private Calendar cal = Calendar.getInstance();		
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		
		int timestamp = StringUtil.convertInt(paraArr[0], 0);
		if(timestamp <= 0){
			return;
		}
		cal.setTimeInMillis(1000L * timestamp);
		int hourOfDay = cal.get(Calendar.HOUR_OF_DAY);
		
		String[] appInfo = paraArr[1].split("\\|");
		if(!appInfo[0].contains("4CD596BDFB0E2D247CB45C8F3072D31D") || appInfo.length < 2){
			return;
		}
		String appID = appInfo[0];
		//String appVersion = appInfo[1];
		String accountID = paraArr[3];
		String platform = paraArr[4];
		if(!"2".equals(platform)){
			return;
		}
		String roomid = AttrUtil.getEventAttrValue(paraArr[paraArr.length - 1], "roomId");
		
		String[] keyFields = new String[] {appID, platform, roomid};
		String[] valFields = new String[] {""+hourOfDay, accountID};

		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
	}
	
	public static void main(String[] args){
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(1000L * 1394550000);
		System.out.println(c.get(Calendar.HOUR_OF_DAY));
	}
}
