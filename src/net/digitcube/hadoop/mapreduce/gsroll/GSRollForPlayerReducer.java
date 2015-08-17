package net.digitcube.hadoop.mapreduce.gsroll;

import java.io.IOException;
import java.util.Date;
import java.util.Map.Entry;
import java.util.TreeMap;
import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.jce.OnlineDay;
import net.digitcube.hadoop.jce.PlayerDayInfo;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.util.DateUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class GSRollForPlayerReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	
	int statDate = 0;
	private Date scheduleTime = null;
	//所有区服按玩家首登日期进行排序
	private TreeMap<Integer, UserInfoRollingLog> gsMap = new TreeMap<Integer, UserInfoRollingLog>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statDate = DateUtil.getStatDate(context.getConfiguration());
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		//清除 gsMap
		gsMap.clear();
		
		//添加到 gsMap
		for(OutFieldsBaseModel val : values){
			UserInfoRollingLog player = new UserInfoRollingLog(scheduleTime, val.getOutFields());
			gsMap.put(player.getPlayerDayInfo().getFirstLoginDate(), player);
		}
		
		//少于或等于一个区服信息，无滚服可算
		if(gsMap.size() <= 1){
			return;
		}
		
		//计算滚服：新增活跃付费及留存等
		for(Entry<Integer, UserInfoRollingLog> toGSInfo : gsMap.entrySet()){
			//玩家当天必须有登录才进行滚服统计
			if(statDate != toGSInfo.getValue().getPlayerDayInfo().getLastLoginDate()){
				continue;
			}
			
			Integer firstLoginDate = toGSInfo.getKey();
			
			//必须 firstLoginDate-1，否则 floorEntry 会返回自己
			Entry<Integer, UserInfoRollingLog> fromGSInfo = gsMap.floorEntry(firstLoginDate-1);
			
			//如果 fromGSInfo 为 null，说明没有从其它服滚到当前服，第一条记录的 fromGSInfo 总是 null
			if(null != fromGSInfo){
				//来源区服名称
				String fromGSName = fromGSInfo.getValue().getPlayerDayInfo().getGameRegion();
				String toGSName = toGSInfo.getValue().getPlayerDayInfo().getGameRegion();
				//两个区服名称不同才需统计，理论上不会出现同名的两个区服
				if(!toGSName.trim().equals(fromGSName.trim())){
					//滚服新增活跃付费等信息
					statGSRollForPlayer(toGSInfo.getValue(), fromGSName, context);
					//滚服 30 日留存信息
					stat30DayRetain(toGSInfo.getValue(), fromGSName, context);
				}
			}
		}
	}

	private void statGSRollForPlayer(UserInfoRollingLog player, String fromGS, Context context) 
			throws IOException, InterruptedException{
		//玩家在当前区服的信息
		PlayerDayInfo playerCurGSInfo = player.getPlayerDayInfo();
		if(null == playerCurGSInfo){
			return;
		}
		
		//是否当天新增
		boolean isNewAdd = statDate == playerCurGSInfo.getFirstLoginDate(); 
		//是否付费
		boolean isEverPay = playerCurGSInfo.getTotalCurrencyAmount() > 0; 
		//新增日付费
		boolean isNewAddDayPay = isNewAdd && isEverPay;
		int newAddDayPayAmount = isNewAddDayPay ? playerCurGSInfo.getTotalCurrencyAmount() : 0;
		//首日付费（不一定新增日）
		boolean isTodayFirstPay = statDate == playerCurGSInfo.getFirstPayDate();
		int firstPayAmount = isTodayFirstPay ? playerCurGSInfo.getTotalCurrencyAmount() : 0;
		//当天是否付费
		boolean isPayToday = statDate == playerCurGSInfo.getLastPayDate();
		int todayPayAmount = 0;
		if(isPayToday){
			for(OnlineDay dayInfo : playerCurGSInfo.getOnlineDayList()){
				if(statDate == dayInfo.getOnlineDate()){
					todayPayAmount = dayInfo.getPayAmount();
					break;
				}
			}
		}
		
		/*
		 * 各字段含义
		 * 0,AppId(和版本号)
		 * 1,平台
		 * 2,当前区渠道
		 * 3,当前服名称
		 * 3,来源服名称
		 * 3,当前玩家帐号ID
		 * 4,当天是否新增
		 * 5,是否付费玩家
		 * 6,是否新增日付费
		 * 7,新增日付费金额
		 * 8,当天是否首次付费
		 * 9,当天首次付费金额
		 * 10,当天是否付费
		 * 11,当天付费金额
		 */
		String[] valFields = new String[]{
					player.getAppID(),
					player.getPlatform(),
					playerCurGSInfo.getChannel(),
					playerCurGSInfo.getGameRegion(),
					fromGS,
					player.getAccountID(),
					isNewAdd ? Constants.DATA_FLAG_YES : Constants.DATA_FLAG_NO,
					isEverPay ? Constants.DATA_FLAG_YES : Constants.DATA_FLAG_NO,
					isNewAddDayPay ? Constants.DATA_FLAG_YES : Constants.DATA_FLAG_NO,
					newAddDayPayAmount+"",
					isTodayFirstPay ? Constants.DATA_FLAG_YES : Constants.DATA_FLAG_NO,
					firstPayAmount+"",
					isPayToday ? Constants.DATA_FLAG_YES : Constants.DATA_FLAG_NO,
					todayPayAmount+""	
		};
		
		keyObj.setOutFields(valFields);
		keyObj.setSuffix(Constants.SUFFIX_GS_ROLL_PLAYER_INFO);
		context.write(keyObj, NullWritable.get());
	}

	// 统计用户流失漏斗
	private void stat30DayRetain(UserInfoRollingLog player, String fromGS, Context context) 
			throws IOException, InterruptedException {
		
		if (statDate != player.getPlayerDayInfo().getLastLoginDate()) {
			// 统计日没有登录，直接返回
			return;
		}
		
		int i = 31;
		while ((--i) > 0) {
			int targetDate = statDate - i * 24 * 3600;
			boolean isLogin = player.isLogin(targetDate, statDate);
			if (!isLogin) { // 是否该天活跃用户
				continue;
			}
			// 是否该天新增用户
			boolean isNewAdd = player.getPlayerDayInfo().getFirstLoginDate() == targetDate;
			// 是否该天付费用户
			boolean isEverPay = player.isPay(targetDate, statDate);
			
			String[] valFields = new String[]{
						player.getAppID(),
						player.getPlatform(),
						player.getPlayerDayInfo().getChannel(),
						player.getPlayerDayInfo().getGameRegion(),
						fromGS,
						player.getAccountID(),
						i+"",
						Constants.PLAYER_TYPE_ONLINE
			};
			
			//a, 活跃玩家留存
			keyObj.setOutFields(valFields);
			keyObj.setSuffix(Constants.SUFFIX_GS_ROLL_30DAY_RETAIN);
			context.write(keyObj, NullWritable.get());
			
			//b, 新增玩家留存
			if(isNewAdd){
				valFields[valFields.length-1] = Constants.PLAYER_TYPE_NEWADD;
				context.write(keyObj, NullWritable.get());
			}
			
			//c, 付费玩家留存
			if(isEverPay){
				valFields[valFields.length-1] = Constants.PLAYER_TYPE_PAYMENT;
				context.write(keyObj, NullWritable.get());
			}
		}
	}

}
