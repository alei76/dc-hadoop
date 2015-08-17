package net.digitcube.hadoop.mapreduce.channel.week;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 输入：滚存输出
 * a) 统计用户的  9 周留存（包括当天新增和活跃用户数，用于留存统计）
 * 
 * 输出：
 * appId, appVer, platform, channel, country, province, playerType, targetDate, offset, playerNum
 */
public class CHWeekRetainMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private IntWritable one = new IntWritable(1);
	
	int statDate = 0;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statDate = DateUtil.getStatWeekDate(context.getConfiguration());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] paramArr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = paramArr[i++];
		String appVer = paramArr[i++];
		String platform = paramArr[i++];
		String channel = paramArr[i++];
		String country = paramArr[i++];
		String province = paramArr[i++];
		String uid = paramArr[i++];
		int firstLoginWeekDate = StringUtil.convertInt(paramArr[i++], 0);
		int onlineWeekTrack = StringUtil.convertInt(paramArr[i++], 0);
		
		//A. 留存统计
		// 当天新增玩家和活跃玩家数量
		String[] keyFields = new String[]{
				appId,
				appVer,
				platform,
				channel,
				country,
				province,
				Constants.PLAYER_TYPE_ONLINE,
				statDate+"",
				"0" // 0 表示当天新增或活跃玩家数
		};
		keyObj.setOutFields(keyFields);
		context.write(keyObj, one);
		//当天新增玩家数量
		if(firstLoginWeekDate == statDate){
			keyFields[keyFields.length - 3] = Constants.PLAYER_TYPE_NEWADD;
			context.write(keyObj, one);
		}
		
		for(int j=1; j<=9; j++){//j=0 是当天的登录情况
			boolean isLogin = ((onlineWeekTrack >> j) & 1) > 0;
			if(!isLogin){
				continue;
			}
			
			//活跃玩家留存
			int targetDate = statDate - 3600 * 24 * 7 *j;
			keyFields = new String[]{
					appId,
					appVer,
					platform,
					channel,
					country,
					province,
					Constants.PLAYER_TYPE_ONLINE,
					targetDate+"",
					j+""
			};
			keyObj.setOutFields(keyFields);
			context.write(keyObj, one);
			//新增玩家留存
			if(firstLoginWeekDate == targetDate){
				keyFields[keyFields.length - 3] = Constants.PLAYER_TYPE_NEWADD;
				context.write(keyObj, one);
			}
		}
	}
}
