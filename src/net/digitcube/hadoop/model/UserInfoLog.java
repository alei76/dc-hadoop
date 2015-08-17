package net.digitcube.hadoop.model;

import net.digitcube.hadoop.util.StringUtil;

public class UserInfoLog extends HeaderLog {

	private int actTime;
	private int regTime;
	private int accountNum;
	
	public int getActTime() {
		return actTime;
	}
	public void setActTime(int actTime) {
		this.actTime = actTime;
	}
	public int getRegTime() {
		return regTime;
	}
	public void setRegTime(int regTime) {
		this.regTime = regTime;
	}
	public int getAccountNum() {
		return accountNum;
	}
	public void setAccountNum(int accountNum) {
		this.accountNum = accountNum;
	}
	
	public UserInfoLog(String[] fields) {
		super(fields);
		int length = fields.length;
		this.actTime = StringUtil.convertInt(fields[length -3], 0);
		this.regTime = StringUtil.convertInt(fields[length -2], 0);
		this.accountNum = StringUtil.convertInt(fields[length -1], 0);
	}
	
	public void setFields(String[] fields){
		super.setFields(fields);
		int length = fields.length;
		this.actTime = StringUtil.convertInt(fields[length -3], 0);
		this.regTime = StringUtil.convertInt(fields[length -2], 0);
		this.accountNum = StringUtil.convertInt(fields[length -1], 0);
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
		arr[17] = ""+this.getActTime();
		arr[18] = ""+this.getRegTime();
		arr[19] = ""+this.getAccountNum();
		return arr;
	}
}
