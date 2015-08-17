package net.digitcube.hadoop.model;

import net.digitcube.hadoop.util.StringUtil;

public class OnlineLog extends HeaderLog {

	private int loginTime;
	private int onlineTime;
	private int level;

	public int getLoginTime() {
		return loginTime;
	}

	public void setLoginTime(int loginTime) {
		this.loginTime = loginTime;
	}

	public int getOnlineTime() {
		return onlineTime;
	}

	public void setOnlineTime(int onlineTime) {
		this.onlineTime = onlineTime;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public OnlineLog(String[] fields) throws Exception {
		super(fields);
		int length = fields.length;
		if(26 != length){
			throw new Exception("Crush data...");
		}
		this.loginTime = StringUtil.convertInt(fields[length -3], 0);
		this.onlineTime = StringUtil.convertInt(fields[length -2], 0);
		this.level = StringUtil.convertInt(fields[length -1], 0);
	}
	
	public void setFields(String[] fields){
		super.setFields(fields);
		int length = fields.length;
		this.loginTime = StringUtil.convertInt(fields[length -3], 0);
		this.onlineTime = StringUtil.convertInt(fields[length -2], 0);
		this.level = StringUtil.convertInt(fields[length -1], 0);
	}
	
	/**
	 * 该方法为临时解决问题用
	 * @return
	 */
	public String[] toOldVersionArr(){
		String[] arr = new String[20];
		arr[0] = this.getTimestamp();
		arr[1] = this.getAppID();
		arr[2] = this.getUID();
		arr[3] = this.getAccountID();
		arr[4] = this.getPlatform();
		arr[5] = this.getChannel();
		arr[6] = this.getAccountType();
		arr[7] = this.getGender();
		arr[8] = this.getAge();
		arr[9] = this.getGameServer();
		arr[10] = this.getResolution();
		arr[11] = this.getOperSystem();
		arr[12] = this.getBrand();
		arr[13] = this.getNetType();
		arr[14] = this.getCountry();
		arr[15] = this.getProvince();
		arr[16] = this.getOperators();
		arr[17] = ""+this.getLoginTime();
		arr[18] = ""+this.getOnlineTime();
		arr[19] = ""+this.getLevel();
		return arr;
	}
}
