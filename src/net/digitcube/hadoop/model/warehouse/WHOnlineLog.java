package net.digitcube.hadoop.model.warehouse;

import net.digitcube.hadoop.util.StringUtil;

public class WHOnlineLog extends WHHeaderLog{

	private int loginTime;
	private int duration;
	private String brand;
	private String resolution;
	private String os;
	private String mac;
	private String imei;
	private String imsi;
	private String idfa;
	private String operator;
	private String netType;
	
	public void setFields(String[] attr){
		super.setFields(attr);
		if(attr.length < 23){
			throw new RuntimeException("Invalid WHOnlineLog, fields is less than 23...");
		}
		int idx = WHHeaderLog.SIZE;
		this.loginTime = StringUtil.convertInt(attr[idx++], 0);
		this.duration = StringUtil.convertInt(attr[idx++], 0);
		this.brand = attr[idx++];
		this.resolution = attr[idx++];
		this.os = attr[idx++];
		this.mac = attr[idx++];
		this.imei = attr[idx++];
		this.imsi = attr[idx++];
		this.idfa = attr[idx++];
		this.operator = attr[idx++];
		this.netType = attr[idx++];
	}
	
	public WHOnlineLog(String[] attr){
		super(attr);
		setFields(attr);
	}

	public int getLoginTime() {
		return loginTime;
	}

	public void setLoginTime(int loginTime) {
		this.loginTime = loginTime;
	}

	public int getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public String getResolution() {
		return resolution;
	}

	public void setResolution(String resolution) {
		this.resolution = resolution;
	}

	public String getOs() {
		return os;
	}

	public void setOs(String os) {
		this.os = os;
	}

	public String getMac() {
		return mac;
	}

	public void setMac(String mac) {
		this.mac = mac;
	}

	public String getImei() {
		return imei;
	}

	public void setImei(String imei) {
		this.imei = imei;
	}

	public String getImsi() {
		return imsi;
	}

	public void setImsi(String imsi) {
		this.imsi = imsi;
	}

	public String getIdfa() {
		return idfa;
	}

	public void setIdfa(String idfa) {
		this.idfa = idfa;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public String getNetType() {
		return netType;
	}

	public void setNetType(String netType) {
		this.netType = netType;
	}
	
}
