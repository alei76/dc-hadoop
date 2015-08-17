package net.digitcube.hadoop.model;

import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.util.StringUtil;

public class ErrorReportLog extends HeaderLog {

	private int errorTime;
	private String MD5;
	private String title;
	private String content;
	
	public int getErrorTime() {
		return errorTime;
	}

	public void setErrorTime(int happenTime) {
		this.errorTime = happenTime;
	}

	public String getMD5() {
		return MD5;
	}

	public void setMD5(String mD5) {
		MD5 = mD5;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public ErrorReportLog(String[] fields) {
		super(fields);
		int length = fields.length;
		this.errorTime = StringUtil.convertInt(fields[length -4], 0);
		this.MD5 = fields[length -3];
		this.title = fields[length -2];
		this.content = fields[length -1];
	}
	
	public void setFields(String[] fields){
		super.setFields(fields);
		int length = fields.length;
		this.errorTime = StringUtil.convertInt(fields[length -4], 0);
		this.MD5 = fields[length -3];
		this.title = fields[length -2];
		this.content = fields[length -1];
	}
	
	/**
	 * 该方法为临时解决问题用
	 * @return
	 */
	public String[] toOldVersionArr(){
		String[] arr = new String[21];
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
		arr[17] = ""+this.getErrorTime();
		arr[18] = this.getMD5();
		arr[19] = this.getTitle();
		arr[20] = this.getContent();
		return arr;
	}
}
