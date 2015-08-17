package net.digitcube.hadoop.mapreduce.gsroll;

import java.io.IOException;
import java.util.Date;
import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.util.DateUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 主要逻辑：
 * 以滚存作为输入，以 appId+platform+accountId 为 key，玩家区服信息为 value
 * 在 reduce 端对统计玩家的所有滚服信息，并输出下列两项滚服相关信息：
 * a)滚服的新增活跃付费等，具体值如下
 * [appId, platform, channel, gameServer, fromGS, accountId, 
 *  isNewAdd, isPay, isNewAddDayPay, newAddDayPayAmount, isTodayFirstPay, firstPayAmount, isPayToday, todayPayAmount]
 *  
 * b)滚服留存情况
 * [appId, platform, channel, gameServer, fromGS, accountId, dayOffSet, playerType]
 */
public class GSRollForPlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

	int statDate = 0;
	private Date scheduleTime = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statDate = DateUtil.getStatDate(context.getConfiguration());
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);

		UserInfoRollingLog player = new UserInfoRollingLog(scheduleTime, arr);
		String gameServer = player.getPlayerDayInfo().getGameRegion();
		//只保留真实区服，计算滚服只需真实区服信息
		if(MRConstants.ALL_GAMESERVER.equals(gameServer)
				|| MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(gameServer)){
			return;
		}
		
		String[] appIdAndVer = player.getAppID().split("\\|");
		if(appIdAndVer.length < 2){
			return;
		}
		String appId = appIdAndVer[0];
		String[] keyFields = new String[]{
				appId,
				player.getPlatform(),
				player.getAccountID()
		};
		
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(arr);
		context.write(mapKeyObj, mapValObj);
	}
}
