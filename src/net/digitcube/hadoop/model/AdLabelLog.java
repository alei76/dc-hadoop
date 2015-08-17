package net.digitcube.hadoop.model;

import net.digitcube.hadoop.util.StringUtil;

public class AdLabelLog {

	private String uid;
	private String platform;
	private String appid;
	private int payAmount;
	private int payTimes;
	private String country;
	private String province;

	public AdLabelLog() {

	}

	public AdLabelLog(String[] parrArr) {
		uid = parrArr[0];
		platform = parrArr[1];
		appid = parrArr[2];
		payAmount = StringUtil.convertInt(parrArr[3], 0);
		payTimes = StringUtil.convertInt(parrArr[4], 0);
		country = parrArr[5];
		province = parrArr[6];
	}

	public String[] toStringArray() {
		return new String[] { uid, platform, appid, payAmount + "",
				payTimes + "", country, province };
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getAppid() {
		return appid;
	}

	public void setAppid(String appid) {
		this.appid = appid;
	}

	public int getPayAmount() {
		return payAmount;
	}

	public void setPayAmount(int payAmount) {
		this.payAmount = payAmount;
	}

	public int getPayTimes() {
		return payTimes;
	}

	public void setPayTimes(int payTimes) {
		this.payTimes = payTimes;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

}
