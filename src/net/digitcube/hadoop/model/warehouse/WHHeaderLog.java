package net.digitcube.hadoop.model.warehouse;

import net.digitcube.hadoop.util.StringUtil;

public class WHHeaderLog {
	public static final int SIZE = 12;
	private String uid;
	private String accountId;
	private String appId;
	private byte platform;
	private String channel;
	private int level;
	private int ts;
	private String ip;
	private String country;
	private String province;
	private String city;
	private String cleanFlag;

	public WHHeaderLog(String[] attr) {
//		setFields(attr);
	}

	public void setFields(String[] attr) {
		if (attr.length < 12) {
			throw new RuntimeException("Invalid WHHeaderLog, fields is less than 12...");
		}
		int idx = 0;
		this.uid = attr[idx++];
		this.accountId = attr[idx++];
		this.appId = attr[idx++];
		this.platform = (byte) StringUtil.convertInt(attr[idx++], 0);
		this.channel = attr[idx++];
		this.level = StringUtil.convertInt(attr[idx++], 0);
		this.ts = StringUtil.convertInt(attr[idx++], 0);
		this.ip = attr[idx++];
		this.country = attr[idx++];
		this.province = attr[idx++];
		this.city = attr[idx++];
		this.cleanFlag = attr[idx++];
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getAccountId() {
		return accountId;
	}

	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public byte getPlatform() {
		return platform;
	}

	public void setPlatform(byte platform) {
		this.platform = platform;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public int getTs() {
		return ts;
	}

	public void setTs(int ts) {
		this.ts = ts;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
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

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getCleanFlag() {
		return cleanFlag;
	}

	public void setCleanFlag(String cleanFlag) {
		this.cleanFlag = cleanFlag;
	}

}
