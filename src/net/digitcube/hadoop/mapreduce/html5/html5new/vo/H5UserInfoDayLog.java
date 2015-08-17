package net.digitcube.hadoop.mapreduce.html5.html5new.vo;

import net.digitcube.hadoop.mapreduce.domain.MapReduceVO;
import net.digitcube.hadoop.util.StringUtil;

public class H5UserInfoDayLog implements MapReduceVO {

	private String appId;
	private String accountId;
	private String platform;
	private String h5app;
	private String h5domain;
	private String h5ref;
	private int h5crtime;
	private String uid;

	private String accountType;
	private String gender;
	private String age;
	private String resolution;
	private String opSystem;
	private String brand;
	private String netType;
	private String country;
	private String province;
	private String operators;
	
	private String parentAccoutId;	

	public H5UserInfoDayLog(){
		
	}

	public H5UserInfoDayLog(String[] args) {
		appId = args[0];
		accountId = args[1];
		platform = args[2];
		h5app = args[3];
		h5domain = args[4];
		h5ref = args[5];
		h5crtime = StringUtil.convertInt(args[6], 0);
		uid = args[7];
		accountType = args[8];
		gender = args[9];
		age = args[10];
		resolution = args[11];
		opSystem = args[12];
		brand = args[13];
		netType = args[14];
		country = args[15];
		province = args[16];
		operators = args[17];
		parentAccoutId = args[18];
	}

	@Override
	public String[] toStringArray() {
		String[] array = new String[19];
		array[0] = appId;
		array[1] = accountId;
		array[2] = platform;
		array[3] = h5app;
		array[4] = h5domain;
		array[5] = h5ref;
		array[6] = h5crtime + "";
		array[7] = uid;
		array[8] = accountType;
		array[9] = gender;
		array[10] = age;
		array[11] = resolution;
		array[12] = opSystem;
		array[13] = brand;
		array[14] = netType;
		array[15] = country;
		array[16] = province;
		array[17] = operators;
		array[18] = parentAccoutId;
		return array;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getAccountId() {
		return accountId;
	}

	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getH5app() {
		return h5app;
	}

	public void setH5app(String h5app) {
		this.h5app = h5app;
	}

	public String getH5domain() {
		return h5domain;
	}

	public void setH5domain(String h5domain) {
		this.h5domain = h5domain;
	}

	public String getH5ref() {
		return h5ref;
	}

	public void setH5ref(String h5ref) {
		this.h5ref = h5ref;
	}

	public int getH5crtime() {
		return h5crtime;
	}

	public void setH5crtime(int h5crtime) {
		this.h5crtime = h5crtime;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getAccountType() {
		return accountType;
	}

	public void setAccountType(String accountType) {
		this.accountType = accountType;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public String getResolution() {
		return resolution;
	}

	public void setResolution(String resolution) {
		this.resolution = resolution;
	}

	public String getOpSystem() {
		return opSystem;
	}

	public void setOpSystem(String opSystem) {
		this.opSystem = opSystem;
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public String getNetType() {
		return netType;
	}

	public void setNetType(String netType) {
		this.netType = netType;
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

	public String getOperators() {
		return operators;
	}

	public void setOperators(String operators) {
		this.operators = operators;
	}
	
	public String getParentAccoutId() {
		return parentAccoutId;
	}

	public void setParentAccoutId(String parentAccoutId) {
		this.parentAccoutId = parentAccoutId;
	}

}
