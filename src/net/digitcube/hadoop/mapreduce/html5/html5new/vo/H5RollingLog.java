package net.digitcube.hadoop.mapreduce.html5.html5new.vo;

import java.util.Date;

import net.digitcube.hadoop.jce.PlayInfoUtil;
import net.digitcube.hadoop.mapreduce.domain.MapReduceVO;

public class H5RollingLog implements MapReduceVO {

	private String appId;
	private String platform;
    private String accountId;
    private String infoBase64;
    private H5PlayerDayInfo playerDayInfo;
    public final static int Index_AppID = 0;
    public final static int Index_Platform = 1;
    public final static int Index_AccountID = 2;
    public final static int Index_InfoBase64 = 3;
    
    private Date scheduleTime = null;
    
    public H5RollingLog(Date scheduleTime){
		if(null == scheduleTime){
			throw new RuntimeException("Schedule time is null, construct H5RollingLog failed...");
		}
		this.scheduleTime = scheduleTime; 
	};
	
	public H5RollingLog(Date scheduleTime, String[] args) {
		if(null == scheduleTime){
			throw new RuntimeException("Schedule time is null, construct H5RollingLog failed...");
		}
		this.scheduleTime = scheduleTime;
		
		this.appId = args[Index_AppID];
		this.platform = args[Index_Platform];
		this.accountId = args[Index_AccountID];
		this.infoBase64 = args[Index_InfoBase64];
	}
	
	@Override
	public String[] toStringArray() {
		this.infoBase64 = PlayInfoUtil.h5PlayDayInfo2Base64(playerDayInfo, scheduleTime);
		return new String[] { appId, platform, accountId, infoBase64 };
	}
	
	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getAccountId() {
		return accountId;
	}

	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}

	public String getInfoBase64() {
		return infoBase64;
	}

	public void setInfoBase64(String infoBase64) {
		this.infoBase64 = infoBase64;
	}

	public H5PlayerDayInfo getPlayerDayInfo() {
		if (playerDayInfo == null) {
			//playerDayInfo = PlayInfoUtil.playDayInfoFromStr(infoBase64);
			playerDayInfo = PlayInfoUtil.h5PlayDayInfoFromStr(infoBase64, scheduleTime);
			
			if (playerDayInfo == null) {
				playerDayInfo = new H5PlayerDayInfo();
			}
		}
		return playerDayInfo;
	}

	public void setPlayerDayInfo(H5PlayerDayInfo playerDayInfo) {
		this.playerDayInfo = playerDayInfo;
	}
	
	/**
	 * @param isLogin
	 */
	public void markLogin(boolean isLogin) {
		playerDayInfo.setTrack(playerDayInfo.getTrack() << 1
				| (isLogin ? 1 : 0));
	}
	
	/**
	 * 判断某天是否登录过
	 * 
	 * @param targetDate
	 *            目标日期，需要判断是否登陆的日期
	 * @param statDate
	 *            最新的统计日期，最后调用markLogin的日期
	 * @return
	 */
	public boolean isLogin(int targetDate, int statDate) {
		// 与最新结算日期的间隔
		int days = (statDate - targetDate) / (24 * 3600);
		if (days < 0 || days > 31) // 超过记录的范围 直接返回false
			return false;
		return (playerDayInfo.getTrack() >> (days) & 1) > 0;
	}
	
	/**
	 * 从统计日期之前N天内是否有登陆过
	 * 
	 * @param everDaysTrack
	 *            everDaysTrack=7 指包含结算日在内的往过去算7天
	 * @return
	 */
	public boolean isEverLogin(int everDaysTrack) {
		if (everDaysTrack > 32) // 超过记录的范围 直接返回false
			return false;
		int track = playerDayInfo.getTrack() << (32 - everDaysTrack);
		return track != 0;
	}
	
	// 记录某天付费
	public void markPay(boolean isPay) {
		playerDayInfo.setPayTrack(playerDayInfo.getPayTrack() << 1
				| (isPay ? 1 : 0));
	}

	// 判断某天是否付费过
	public boolean isPay(int targetDate, int statDate) {
		// 与最新日期的间隔
		int days = (statDate - targetDate) / (24 * 3600);
		if (days < 0 || days > 31) // 超过记录的范围 直接返回false
			return false;
		return (playerDayInfo.getPayTrack() >> (days) & 1) > 0;
	}
	
	public static String parseInt2Binary(int intVal) {
		String s = "";
		for (int i = 31; i >= 0; i--) {
			s += intVal >> i & 1;
		}
		return s;
	}

}
