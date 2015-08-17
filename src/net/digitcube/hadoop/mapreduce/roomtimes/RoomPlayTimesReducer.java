package net.digitcube.hadoop.mapreduce.roomtimes;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 房间局数统计Reducer-2
 * 
 * Title: RoomPlayTimesReducer.java<br>
 * Description: RoomPlayTimesReducer.java<br>
 * Copyright: Copyright (c) 2013 www.dataeye.com<br>
 * Company: DataEye 数据之眼<br>
 * 
 * @author Ivan     <br>
 * @date 2013-10-31         <br>
 * @version 1.0          <br>
 * 
 * 其依赖的Map 输出
 *  key:
 * [appID,platform,channel,gameserver,roomid]
 *  value:
 * [sum(new),sum(active),sum(pay)]
 * 
 * Reduce 输出：
 * suffix:ROOM_PLAYTIMES
 * [appID,platform,channel,gameserver,roomid,sum(新增用户次数),sum(活跃用户次数),sum(付费用户次数)] SUFFIX_ROOM_PLAYTIMES
 */
public class RoomPlayTimesReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		int newUserPlayTimes = 0;
		int activeUserPlayTimes = 0;
		int payUserPlayTimes = 0;

		for (OutFieldsBaseModel value : values) {
			String[] allcounter = value.getOutFields();
			newUserPlayTimes += StringUtil.convertInt(allcounter[0], 0);
			activeUserPlayTimes += StringUtil.convertInt(allcounter[1], 0);
			payUserPlayTimes += StringUtil.convertInt(allcounter[2], 0);
		}
		key.setSuffix(Constants.SUFFIX_ROOM_PLAYTIMES);
		
		
		//新增
		String[] outputValues = new String[] { 
				Constants.PLAYER_TYPE_NEWADD,
				Constants.DIMENSION_ROOM_PLAYTIMES,
				newUserPlayTimes + ""
		};
		valObj.setOutFields(outputValues);
		context.write(key, valObj);
		
		//活跃
		outputValues = new String[] { 
				Constants.PLAYER_TYPE_ONLINE,
				Constants.DIMENSION_ROOM_PLAYTIMES, 
				activeUserPlayTimes + ""
		};
		valObj.setOutFields(outputValues);
		context.write(key, valObj);
		
		//付费
		outputValues = new String[] { 
				Constants.PLAYER_TYPE_PAYMENT,
				Constants.DIMENSION_ROOM_PLAYTIMES, 
				payUserPlayTimes + "" 
		};
		valObj.setOutFields(outputValues);
		context.write(key, valObj);
	}
}
