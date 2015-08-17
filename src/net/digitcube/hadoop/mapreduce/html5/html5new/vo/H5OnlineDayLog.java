package net.digitcube.hadoop.mapreduce.html5.html5new.vo;

import net.digitcube.hadoop.mapreduce.domain.MapReduceVO;
import net.digitcube.hadoop.util.StringUtil;

public class H5OnlineDayLog implements MapReduceVO {
	
	private String appId;
	private String accountId;
	private String platform; 
	private String h5app;
	private String h5domain;
	private String h5ref;
	private int h5crtime;
	private String uid;
	private int totalLoginTimes;
	private int totalOnlineTime;
	private int uniqIpCount;
	private String onlineRecords;
	private String ipRecords;	
	private int lastLoginTime;// (最后登陆时间)

	private String accountType ;
	private String gender ;
	private String age ;		
	private String resolution ;
	private String opSystem ;
	private String brand ;
	private String netType ;
	private String country ;
	private String province ;
	private String operators ;
	private int level ;
	private String playerType;

	public H5OnlineDayLog(){
    	
    }
    
    public H5OnlineDayLog(String[] args) {
    	appId = args[0];
    	accountId = args[1];
    	platform = args[2];
    	h5app = args[3];
    	h5domain = args[4];
    	h5ref = args[5];
    	h5crtime = StringUtil.convertInt(args[6],0);
    	uid = args[7];
    	totalLoginTimes = StringUtil.convertInt(args[8],0);
    	totalOnlineTime = StringUtil.convertInt(args[9],0);
    	uniqIpCount = StringUtil.convertInt(args[10],0);
    	onlineRecords = args[11];
    	ipRecords = args[12];
    	lastLoginTime = StringUtil.convertInt(args[13],0);
    	accountType = args[14];
    	gender = args[15];
    	age = args[16];
    	resolution = args[17];
    	opSystem = args[18];
    	brand = args[19];
    	netType = args[20];
    	country = args[21];
    	province = args[22];
    	operators = args[23];
    	level = StringUtil.convertInt(args[24],0);
    	if(args.length > 25){
    		playerType = args[25];
    	}
    }
    
	@Override
	public String[] toStringArray() {
		String[] array = new String[26];
		array[0] = appId;
		array[1] = accountId;
		array[2] = platform;
		array[3] = h5app;
		array[4] = h5domain;
		array[5] = h5ref;
		array[6] = h5crtime + "";
		array[7] = uid;
		array[8] = totalLoginTimes + "";
		array[9] = totalOnlineTime + "";
		array[10] =uniqIpCount + "";
		array[11] = onlineRecords;
		array[12] = ipRecords;
		array[13] = lastLoginTime + "";		
		array[14] = accountType; 
		array[15] = gender; 
		array[16] = age;
		array[17] = resolution;
		array[18] = opSystem;
		array[19] = brand; 
		array[20] = netType;
		array[21] = country;
		array[22] = province;
		array[23] = operators;	
		array[24] = level + "";
		array[25] = playerType;
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

	public int getTotalLoginTimes() {
		return totalLoginTimes;
	}

	public void setTotalLoginTimes(int totalLoginTimes) {
		this.totalLoginTimes = totalLoginTimes;
	}

	public int getTotalOnlineTime() {
		return totalOnlineTime;
	}

	public void setTotalOnlineTime(int totalOnlineTime) {
		this.totalOnlineTime = totalOnlineTime;
	}

	public int getUniqIpCount() {
		return uniqIpCount;
	}

	public void setUniqIpCount(int uniqIpCount) {
		this.uniqIpCount = uniqIpCount;
	}

	public String getOnlineRecords() {
		return onlineRecords;
	}

	public void setOnlineRecords(String onlineRecords) {
		this.onlineRecords = onlineRecords;
	}

	public String getIpRecords() {
		return ipRecords;
	}

	public void setIpRecords(String ipRecords) {
		this.ipRecords = ipRecords;
	}	
	
	public int getLastLoginTime() {
		return lastLoginTime;
	}

	public void setLastLoginTime(int lastLoginTime) {
		this.lastLoginTime = lastLoginTime;
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
	
    public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}
	
	public String getPlayerType() {
		return playerType;
	}

	public void setPlayerType(String playerType) {
		this.playerType = playerType;
	}
	
}
