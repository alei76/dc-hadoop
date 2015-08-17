package net.digitcube.hadoop.model;

public class ClickLog {
	private String timestamp;
	private String slCode;
	private String ip;
	private String appid;
	private String channel;
	private String isIOS;
	private String device;
	private String os;
	private String country;
	private String province;
	private String city;
	private String operator;
	private String ua;
	private String appUrl;

	public ClickLog(String[] fields) {
		setFields(fields);
	}

	public void setFields(String[] fields) {
		this.timestamp = fields[0];
		this.slCode = fields[1];
		this.ip = fields[2];
		this.appid = fields[3];
		this.channel = fields[4];
		this.isIOS = fields[5];
		this.device = fields[6];
		this.os = fields[7];
		this.country = fields[8];
		this.province = fields[9];
		this.city = fields[10];
		this.operator = fields[11];
		this.ua = fields[12];
		this.appUrl = fields[13];
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getSlCode() {
		return slCode;
	}

	public void setSlCode(String slCode) {
		this.slCode = slCode;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getAppid() {
		return appid;
	}

	public void setAppid(String appid) {
		this.appid = appid;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public boolean isIOS() {
		return "1".equals(isIOS);
	}

	public String getIsIOS() {
		return isIOS;
	}

	public void setIsIOS(String isIOS) {
		this.isIOS = isIOS;
	}

	public String getDevice() {
		return device;
	}

	public void setDevice(String device) {
		this.device = device;
	}

	public String getOs() {
		return os;
	}

	public void setOs(String os) {
		this.os = os;
	}

	public String getUa() {
		return ua;
	}

	public void setUa(String ua) {
		this.ua = ua;
	}

	public String getAppUrl() {
		return appUrl;
	}

	public void setAppUrl(String appUrl) {
		this.appUrl = appUrl;
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

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

}
