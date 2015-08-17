package net.digitcube.hadoop.mapreduce.html5.html5new;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5OnlineDay;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5OnlineDayLog;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5PvDayLog;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5RollingLog;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class H5UserRollingDayReducer extends
			Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {
	
	// 加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = null;
	
	// 保存同一个帐号对应多个设备的 UID
	// 同一个帐号可能在多台设备上进行游戏
	private Set<String> uidSet = new HashSet<String>();
	
	// 统计的数据时间
	private int statTime = 0;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(),	new Date());
		statTime = DateUtil.getStatDateForHourOrToday(context.getConfiguration());
	}
	
	private Context context;
	
	@Override
	protected void reduce(OutFieldsBaseModel key,Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		this.context = context;
		uidSet.clear();
		// 本次登陆的在线时长
		int onlineTime = 0;
		// 本次结算的登陆次数
		int totalLoginTimes = 0;
		// 本次结算的付费数量
		int totalCurrencyAmount = 0;
		// 本次登陆之前的最后一次登陆日期，即，若本次有登陆，则是倒数第二次登陆，否则，仍是最后一次登陆
		int lastLoginDate = 0;
		// 本次付费之前的最后一次付费，即，若本次有付费，则是倒数第二次付费，否则，仍是最后一次付费		
		int lastPayDate = 0;		
		// 滚存中取值优先级：在线日志中版本号 > pv中版本号 > 注册日志中版本号 > 滚存日志中版本号
		String newestVersion = null;		
		String[] appVerArr = new String[4];
		
		String[] h5UserRollArray = null;
		String[] onlineArray = null;
		String[] pvArray = null;
		String[] userInfoArray = null;
		H5OnlineDayLog h5OnlineDayLog = null;
		H5PvDayLog h5PvDayLog = null;
		H5OnlineDay h5OnlineDay = null;
		String uid = null;
		int createTime = 0;
		String parentAccountId = "";
		String platform = "1";
		for (OutFieldsBaseModel value : values) {
			String[] array = value.getOutFields();
			if ("U".equals(array[0])) {
				h5UserRollArray = array;
				// 设置 APP 版本号
				appVerArr[0] = h5UserRollArray[2];
			} else if ("O".equals(value.getSuffix())) {
				onlineArray = array;
				h5OnlineDayLog = new H5OnlineDayLog(array);
				String[] arr = h5OnlineDayLog.getAppId().split("\\|");
				if (arr.length > 1) {
					appVerArr[1] = arr[1];
				}
				if(!StringUtil.isEmpty(h5OnlineDayLog.getUid())){
					uid = h5OnlineDayLog.getUid();
				}				
				createTime = h5OnlineDayLog.getH5crtime();	
				platform = h5OnlineDayLog.getPlatform();
			}else if("PV".equals(value.getSuffix())){
				pvArray = array;
				h5PvDayLog = new H5PvDayLog(array);
				String[] arr = h5PvDayLog.getAppId().split("\\|");
				if (arr.length > 1) {
					appVerArr[2] = arr[1];
				}
				if(!StringUtil.isEmpty(h5PvDayLog.getUid())){
					uid = h5PvDayLog.getUid();
				}
				createTime = h5PvDayLog.getH5crtime();	
				platform = h5PvDayLog.getPlatform();
			}else if("R".equals(value.getSuffix())){
				userInfoArray = array;
				// 新注册的玩家滚存里还不存在，取注册激活里日志里的 APP 版本号
				appVerArr[3] = userInfoArray[userInfoArray.length - 1];
				// UID
				if(!StringUtil.isEmpty(userInfoArray[userInfoArray.length - 3])){
					uid = userInfoArray[userInfoArray.length - 3];
				}
				parentAccountId = userInfoArray[userInfoArray.length - 2];
				platform = userInfoArray[1];
			}	
		
		}
		
		if(!StringUtil.isEmpty(uid)){
			uidSet.add(uid);
		}
		
		// 优先级取 app 版本号
		// 在线日志中版本号 > pv中版本号 > 注册日志中版本号 > 滚存日志中版本号
		if (null != appVerArr[1]) {
			newestVersion = appVerArr[1];
		} else if (null != appVerArr[2]) {
			newestVersion = appVerArr[2];
		} else if (null != appVerArr[3]) {
			newestVersion = appVerArr[3];
		} else {
			newestVersion = appVerArr[0];
		}
		if (null == newestVersion || "".equals(newestVersion)) {
			newestVersion = "1.0";
		}
		
		H5RollingLog h5RollingLog = new H5RollingLog(scheduleTime);
		String[] keyArray = key.getOutFields();
		
		// map 端已经把 appId 和 version 拆分开，这里需合并
		// 以兼容依赖于滚存的 MR
		h5RollingLog.setAppId(keyArray[0] + "|" + newestVersion);
		h5RollingLog.setAccountId(keyArray[1]);
		h5RollingLog.setPlatform(platform);
		if (h5UserRollArray != null) {
			h5RollingLog.setInfoBase64(h5UserRollArray[1]);
		}	
		
		if (userInfoArray != null) {
			h5RollingLog.getPlayerDayInfo().setParentId(userInfoArray[6]);			
		}
		
		// 倒数第二次登陆
		lastLoginDate = h5RollingLog.getPlayerDayInfo().getLastLoginDate();
		if(onlineArray != null){
			
			h5RollingLog.getPlayerDayInfo().setPlatform(StringUtil.convertInt(platform,1));
			h5RollingLog.getPlayerDayInfo().setUid(uid);
			h5RollingLog.getPlayerDayInfo().setCreateTime(createTime);
			
			if (h5RollingLog.getPlayerDayInfo().getFirstLoginDate() == 0) {
				// 如果首登时间为 0 说明是新增玩家，用 statDate 作为首登日期
				// 首登日期
				h5RollingLog.getPlayerDayInfo().setFirstLoginDate(statTime);			
			}
			// 记录最后一次登录时间
			h5RollingLog.getPlayerDayInfo().setLastLoginDate(statTime);
			
			h5RollingLog.getPlayerDayInfo().setDomain(h5OnlineDayLog.getH5domain());
			h5RollingLog.getPlayerDayInfo().setRefer(h5OnlineDayLog.getH5ref());
			h5RollingLog.getPlayerDayInfo().setPromptApp(h5OnlineDayLog.getH5app());
			
			// 记录总在线时长
			onlineTime = h5OnlineDayLog.getTotalOnlineTime();
			h5RollingLog.getPlayerDayInfo().setTotalOnlineTime(h5RollingLog.getPlayerDayInfo().getTotalOnlineTime()
					 + onlineTime);
			// 记录总登陆次数
			totalLoginTimes = h5OnlineDayLog.getTotalLoginTimes();
			h5RollingLog.getPlayerDayInfo().setTotalLoginTimes(h5RollingLog.getPlayerDayInfo().getTotalLoginTimes() 
					+ totalLoginTimes);
			// 记录已玩天数
			h5RollingLog.getPlayerDayInfo().setTotalOnlineDay(h5RollingLog.getPlayerDayInfo().getTotalOnlineDay() + 1);
			
			h5OnlineDay = h5OnlineDay == null ? new H5OnlineDay() : h5OnlineDay;
			h5OnlineDay.setLoginTimes(totalLoginTimes);
			h5OnlineDay.setOnlineDate(statTime);
			h5OnlineDay.setOnlineTime(onlineTime);
			
		}
		
		if(pvArray != null){
			h5RollingLog.getPlayerDayInfo().setDomain(h5PvDayLog.getH5domain());
			h5RollingLog.getPlayerDayInfo().setRefer(h5PvDayLog.getH5ref());
			h5RollingLog.getPlayerDayInfo().setPromptApp(h5PvDayLog.getH5app());
			
			if (h5RollingLog.getPlayerDayInfo().getFirstLoginDate() == 0) {
				// 如果首登时间为 0 说明是新增玩家，用 statDate 作为首登日期
				// 首登日期
				h5RollingLog.getPlayerDayInfo().setFirstLoginDate(statTime);			
			}
			
		}
		
		if(onlineArray != null){
			if(!StringUtil.isEmpty(h5RollingLog.getPlayerDayInfo().getDomain())){
				h5RollingLog.getPlayerDayInfo().setDomain(h5OnlineDayLog.getH5domain());
			}
			if(!StringUtil.isEmpty(h5RollingLog.getPlayerDayInfo().getRefer())){
				h5RollingLog.getPlayerDayInfo().setRefer(h5OnlineDayLog.getH5ref());
			}
			if(!StringUtil.isEmpty(h5RollingLog.getPlayerDayInfo().getPromptApp())){
				h5RollingLog.getPlayerDayInfo().setPromptApp(h5OnlineDayLog.getH5app());
			}			
		}
		
		if(userInfoArray != null){
			if (h5RollingLog.getPlayerDayInfo().getFirstLoginDate() == 0) {
				// 如果首登时间为 0 说明是新增玩家，用 statDate 作为首登日期
				// 首登日期
				h5RollingLog.getPlayerDayInfo().setFirstLoginDate(statTime);			
			}
			h5RollingLog.getPlayerDayInfo().setParentId(parentAccountId);
		}		
		
		
		if(h5OnlineDay != null) {
			ArrayList<H5OnlineDay> onlineDayList = h5RollingLog.getPlayerDayInfo().getOnlineDayList();
			onlineDayList = onlineDayList == null ? new ArrayList<H5OnlineDay>() : onlineDayList; 
			onlineDayList.add(h5OnlineDay);
			h5RollingLog.getPlayerDayInfo().setOnlineDayList(onlineDayList); 
		}
		
		// 记录用户32天内的登陆情况
		h5RollingLog.markLogin(totalLoginTimes > 0);
		// 记录用户32天内的付费情况
		h5RollingLog.markPay(totalCurrencyAmount > 0);
		
		// 是否新增
		boolean isNewPlayer = statTime == h5RollingLog.getPlayerDayInfo()
				.getFirstLoginDate();
		// 是否该天活跃用户
		boolean isOnline = statTime == h5RollingLog.getPlayerDayInfo()
							.getLastLoginDate()  ;
		// 是否该天付费用户
		boolean isPayUser = statTime == h5RollingLog.getPlayerDayInfo()
							.getLastPayDate()  ;	
		
		// 活跃、新增
		statOnlineDayApp(isNewPlayer,isOnline,h5OnlineDayLog,h5RollingLog);
		//  pv	
		if(null != h5PvDayLog){
			statPageViewForApp(isNewPlayer,isOnline,h5PvDayLog,h5RollingLog);
		}
		//  病毒传播		
		if(StringUtil.isEmpty(parentAccountId)){
			parentAccountId = MRConstants.INVALID_PLACE_HOLDER_CHAR;
		}
		statVirusSpreadForApp(parentAccountId,h5OnlineDayLog,h5PvDayLog,h5RollingLog);
		
		// 统计流失漏斗
		if(null != h5RollingLog){
			statUserLostFunnel(statTime, h5RollingLog);
		}
		// 输出玩家在线信息，并标识是否新增、付费
		if (null != h5OnlineDayLog && null != onlineArray) {
			statPlayerOnlineInfo(statTime,isNewPlayer,isPayUser,h5OnlineDayLog,h5RollingLog) ;
		}
		
		if(StringUtil.isEmpty(h5RollingLog.getPlayerDayInfo().getParentId())){
			h5RollingLog.getPlayerDayInfo().setParentId(MRConstants.INVALID_PLACE_HOLDER_CHAR);
		}
		
		//记录用户滚存记录
		key.setOutFields(h5RollingLog.toStringArray());
		key.setSuffix(Constants.SUFFIX_H5_USERROLLING);
		context.write(key, NullWritable.get());
	
	}
	
	
	/**
	 * onlineDayApp活跃、新增
	 * @param isNewPlayer
	 * @param h5RollingLog
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void statOnlineDayApp(boolean isNewPlayer,boolean isOnline, H5OnlineDayLog h5OnlineDayLog,H5RollingLog h5RollingLog) throws IOException,
						InterruptedException {
		OutFieldsBaseModel outkey = new OutFieldsBaseModel();
		outkey.setSuffix(Constants.SUFFIX_H5_NEW_STAT_ONLINEDAY_APP);		
		
		if(isNewPlayer){
			outkey.setOutFields(getStardKeyWithArgs(h5RollingLog,
					Constants.PLAYER_TYPE_NEWADD, 
					h5OnlineDayLog!=null?h5OnlineDayLog.getTotalLoginTimes():0,
					h5OnlineDayLog!=null?h5OnlineDayLog.getTotalOnlineTime():0,
					h5OnlineDayLog!=null?h5OnlineDayLog.getIpRecords():"[]"));
			context.write(outkey, NullWritable.get());
		}
		
		if(isOnline){
			outkey.setOutFields(getStardKeyWithArgs(h5RollingLog,
					Constants.PLAYER_TYPE_ONLINE,
					h5OnlineDayLog!=null?h5OnlineDayLog.getTotalLoginTimes():0,
					h5OnlineDayLog!=null?h5OnlineDayLog.getTotalOnlineTime():0,
					h5OnlineDayLog!=null?h5OnlineDayLog.getIpRecords():"[]"));
			context.write(outkey, NullWritable.get());
		}		
	}
	
	/**
	 * pv
	 * @param isNewPlayer
	 * @param h5PvDayLog
	 * @param h5RollingLog
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void statPageViewForApp(boolean isNewPlayer,boolean isOnline, H5PvDayLog h5PvDayLog,H5RollingLog h5RollingLog) throws IOException,
						InterruptedException {
		OutFieldsBaseModel outkey = new OutFieldsBaseModel();
		outkey.setSuffix(Constants.SUFFIX_H5_NEW_STAT_PV_APP);	
		if(isNewPlayer){
			outkey.setOutFields(getStardKeyWithArgs(h5RollingLog,
					Constants.PLAYER_TYPE_NEWADD, 
					h5PvDayLog.getPlayerTotalPVs(),
					h5PvDayLog.getPlayer1ViewPvs(),
					h5PvDayLog.getPlayerPVRecords()));
			context.write(outkey, NullWritable.get());	
		}
		if(isOnline){
			outkey.setOutFields(getStardKeyWithArgs(h5RollingLog,
					Constants.PLAYER_TYPE_ONLINE, 
					h5PvDayLog.getPlayerTotalPVs(),
					h5PvDayLog.getPlayer1ViewPvs(),
					h5PvDayLog.getPlayerPVRecords()));
			context.write(outkey, NullWritable.get());	
		}
		
	}
	
	/**
	 * 病毒传播
	 * @param parentAccountId
	 * @param h5OnlineDayLog
	 * @param h5PvDayLog
	 * @param h5RollingLog
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void statVirusSpreadForApp(String parentAccountId,H5OnlineDayLog h5OnlineDayLog, 
						H5PvDayLog h5PvDayLog,H5RollingLog h5RollingLog) throws IOException,
						InterruptedException {
		OutFieldsBaseModel outkey = new OutFieldsBaseModel();
		outkey.setSuffix(Constants.SUFFIX_H5_NEW_VIRUS_SPREAD);		
		outkey.setOutFields(getStardKeyWithArgs(h5RollingLog,
				parentAccountId,
				h5OnlineDayLog!=null?h5OnlineDayLog.getTotalLoginTimes():0,
				h5OnlineDayLog!=null?h5OnlineDayLog.getTotalOnlineTime():0,
				h5PvDayLog!=null?h5PvDayLog.getPlayerTotalPVs():0,						
				h5OnlineDayLog!=null?h5OnlineDayLog.getIpRecords():"[]"));
		context.write(outkey, NullWritable.get());	
	}
	
	// 统计用户流失漏斗
	private void statUserLostFunnel(int statTime,H5RollingLog h5RollingLog) throws IOException,
				InterruptedException {
		if (statTime != h5RollingLog.getPlayerDayInfo()
				.getLastLoginDate()) {
			// 统计日没有登录，直接返回
			return;
		}
		int i = 31;
		while ((--i) > 0) {
			int targetDate = statTime - i * 24 * 3600;
			boolean isLogin = h5RollingLog.isLogin(targetDate, statTime);
			if (!isLogin) {
				// 是否该天活跃用户
				continue;
			}
			// 是否该天新增用户
			boolean isNewUser = h5RollingLog.getPlayerDayInfo()
					.getFirstLoginDate() == targetDate;
			// 是否该天付费用户
			boolean isPayUser = h5RollingLog.isPay(targetDate, statTime);
			OutFieldsBaseModel key = new OutFieldsBaseModel();
			key.setSuffix(Constants.SUFFIX_H5_USER_LOST_FUNNEL);
			if (isNewUser) {
				key.setOutFields(getStardKeyWithArgs(h5RollingLog,
						Constants.PLAYER_TYPE_NEWADD, i));
				context.write(key, NullWritable.get());
			}
			if (isPayUser) {
				key.setOutFields(getStardKeyWithArgs(h5RollingLog,
						Constants.PLAYER_TYPE_PAYMENT, i));
				context.write(key, NullWritable.get());
			}
			key.setOutFields(getStardKeyWithArgs(h5RollingLog,
					Constants.PLAYER_TYPE_ONLINE, i));
			context.write(key, NullWritable.get());
		}
	}	
	
	// 输出玩家在线信息，并标识是否新增、付费
	private void statPlayerOnlineInfo(int statTime,boolean isNewPlayer,
					boolean isPayUser,H5OnlineDayLog h5OnlineDayLog,H5RollingLog h5RollingLog) throws IOException,
			InterruptedException {		
		String playerType = Constants.DATA_FLAG_PLAYER_ONLINE; // 只是活跃		
		if (isNewPlayer && isPayUser) { // 集新增、活跃、付费于一身
			playerType = Constants.DATA_FLAG_PLAYER_NEW_ONLINE_PAY;
		} else if (isNewPlayer) { // 只是新增、活跃
			playerType = Constants.DATA_FLAG_PLAYER_NEW_ONLINE;
		} else if (isPayUser) { // 只是活跃、付费
			playerType = Constants.DATA_FLAG_PLAYER_ONLINE_PAY;
		}
		
		String[] onlineArray = h5OnlineDayLog.toStringArray();
		int length = onlineArray.length + 1;
		String[] onlineInfoArr = new String[length];
		System.arraycopy(onlineArray, 0, onlineInfoArr, 0, onlineArray.length);
		onlineInfoArr[length - 1] = playerType;

		OutFieldsBaseModel outKey = new OutFieldsBaseModel();
		outKey.setSuffix(Constants.SUFFIX_H5_NEW_PLAYER_ONLINE_INFO);
		outKey.setOutFields(onlineInfoArr);
		context.write(outKey, NullWritable.get());		
	}
	
	
	
	private String[] getStardKeyWithArgs(H5RollingLog h5RollingLog,	Object... args) {
		List<String> list = new ArrayList<String>();
		list.add(h5RollingLog.getAppId());
		list.add(h5RollingLog.getAccountId());
		list.add(h5RollingLog.getPlatform());
		list.add(h5RollingLog.getPlayerDayInfo().getPromptApp());
		list.add(h5RollingLog.getPlayerDayInfo().getDomain());
		list.add(h5RollingLog.getPlayerDayInfo().getRefer());
		if (args != null) {
			for (Object arg : args) {
				list.add(arg + "");
			}
		}
		return list.toArray(new String[0]);
	}
	

}
