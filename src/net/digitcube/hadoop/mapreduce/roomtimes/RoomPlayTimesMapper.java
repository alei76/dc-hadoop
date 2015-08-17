package net.digitcube.hadoop.mapreduce.roomtimes;

import java.io.IOException;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
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
public class RoomPlayTimesMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String[] keyFields = null;
		String[] valueFields = null;

		keyFields = new String[] { paraArr[0], paraArr[1], paraArr[3], paraArr[4], paraArr[9] };
		valueFields = new String[] { paraArr[10], paraArr[11], paraArr[12] };

		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valueFields);
		context.write(keyObj, valObj);
	}
}
