package net.digitcube.hadoop.mapreduce.onlinenew;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author rickpan
 * @version 1.0 
 * 输入： <br/>
 * key	:	
 * APPID,Platform,AccountID,LoginTime
 * 
 * value(玩家一条完整在线日志):	
 * Timestamp,APPID,UID,AccountID,Platform,
 * Channel,AccountType,Gender,Age,GameServer,Resolution,OperSystem,Brand,NetType,Country,Province,Operators,
 * LoginTime,OnlineTime,Level
 * 
 * 输出： 玩家同一次登录的去重结果(取最大 OnlineTime 和最大 Level)<br/>
 * Timestamp,APPID,UID,AccountID,Platform,
 * Channel,AccountType,Gender,Age,GameServer,Resolution,OperSystem,Brand,NetType,Country,Province,Operators,
 * LoginTime,max(OnlineTime),max(Level)
 */

public class OnlineHourReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel reduceKeyObj = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {

		// 计算用户最高等级，最大在线时间
		int maxLevel = 0;
		int maxOnline = 0;
		String[] lastValue = null;
		for (OutFieldsBaseModel val : values) {
			lastValue = val.getOutFields();
			int level = StringUtil.convertInt(lastValue[Constants.INDEX_ONLINE_LEVEL], 0);// level
			int online = StringUtil.convertInt(lastValue[Constants.INDEX_ONLINE_TIME], 0);// online time
			maxLevel = maxLevel > level ? maxLevel : level;
			maxOnline = maxOnline > online ? maxOnline : online;
		}
		
		lastValue[Constants.INDEX_ONLINE_LEVEL] = "" + maxLevel;
		lastValue[Constants.INDEX_ONLINE_TIME] = "" + maxOnline;
		
		reduceKeyObj.setOutFields(lastValue);
		reduceKeyObj.setSuffix(Constants.SUFFIX_ONLINE_HOUR);
		
		context.write(reduceKeyObj, NullWritable.get());
	}
}
