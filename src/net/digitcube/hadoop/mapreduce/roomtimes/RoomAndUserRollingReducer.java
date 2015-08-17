package net.digitcube.hadoop.mapreduce.roomtimes;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 房间游戏局数统计 Reducer-1
 * 
 * 
 * Title: RoomAndUserRollingReducer.java<br>
 * Description: RoomAndUserRollingReducer.java<br>
 * Copyright: Copyright (c) 2013 www.dataeye.com<br>
 * Company: DataEye 数据之眼<br>
 * 
 * @author Ivan     <br>
 * @date 2013-10-31         <br>
 * @version 1.0          <br>
 * 
 * 
 *  如果是滚存日志：         
 *  Map:
 *       key = [ appID, platform, accountID ]
 *       value = [ 当前用户的所有类型身份] suffix:USER_TYPE
 *  如果是房间局数日志
 *  	 key = [ appID, platform, accountID ]
 *       value=  [channel, gameServer, accountType, gender, age, province, roomid] suffix:ROOM_DATA
 * 
 * 
 * 该Reducer 输出:
 * [ appID, platform, accountID,channel,gameserver,accountType, gender, age, province,roomid,sum(new),sum(active),sum(pay)] suffix:ROOM_DATA
 */
public class RoomAndUserRollingReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	private Map<String, Integer> roomCount = new HashMap<String, Integer>();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		roomCount.clear();
		String[] keyArr = key.getOutFields();
		List<String> userType = null;
		String[] keyFields = null;
		for (OutFieldsBaseModel outfields : values) {
			if (outfields.getSuffix().equals(Constants.SUFFIX_USER_TYPE) && userType == null) {
				userType = Arrays.asList(outfields.getOutFields());
			} else if (outfields.getSuffix().equals(Constants.SUFFIX_ROOM_DATA)) {
				String[] fields = outfields.getOutFields();
				if (keyFields == null) {
					keyFields = new String[] { keyArr[0], keyArr[1], keyArr[2], fields[0], fields[1], fields[2],
							fields[3], fields[4], fields[5], };
				}
				String roomid = fields[6]; // roomid
				Integer count = roomCount.get(roomid) == null ? 0 : roomCount.get(roomid);
				count++;
				roomCount.put(roomid, count);
			}
		}
		if (userType == null || keyFields == null)
			return;
		for (Map.Entry<String, Integer> entry : roomCount.entrySet()) {
			String roomid = entry.getKey();
			int count = entry.getValue();
			String[] valueFields = new String[4];
			valueFields[0] = roomid;
			if (userType.contains(Constants.PLAYER_TYPE_NEWADD)) {
				valueFields[1] = count + "";
			} else {
				valueFields[1] = "0";
			}
			if (userType.contains(Constants.PLAYER_TYPE_ONLINE)) {
				valueFields[2] = count + "";
			} else {
				valueFields[2] = "0";
			}
			if (userType.contains(Constants.PLAYER_TYPE_PAYMENT)) {
				valueFields[3] = count + "";
			} else {
				valueFields[3] = "0";
			}
			keyObj.setOutFields(keyFields);
			keyObj.setSuffix(Constants.SUFFIX_ROOM_DATA);
			valObj.setOutFields(valueFields);
			context.write(keyObj, valObj);
		}

	}
}
