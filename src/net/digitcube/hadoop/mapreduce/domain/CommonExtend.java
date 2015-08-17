package net.digitcube.hadoop.mapreduce.domain;

import net.digitcube.hadoop.common.Constants;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月17日 下午7:36:17 @copyrigt www.digitcube.net
 */

public class CommonExtend implements MapReduceVO {
	private String channel;
	private String accountType;
	private String gender;
	private String age;
	private String gameServer;
	private String resolution;
	private String operSystem;
	private String brand;
	private String netType;
	private String country;
	private String province;
	private String operators;

	public CommonExtend(){}
	public CommonExtend(String[] args) {
		this(args, 0);
	}

	public CommonExtend(String[] args, int offset) {
		channel = args[Constants.INDEX_CHANNEL + offset];
		accountType = args[Constants.INDEX_ACCOUNTTYPE + offset];
		gender = args[Constants.INDEX_GENDER + offset];
		age = args[Constants.INDEX_AGE + offset];
		gameServer = args[Constants.INDEX_GAMESERVER + offset];
		resolution = args[Constants.INDEX_RESOLUTION + offset];
		operSystem = args[Constants.INDEX_OPERSYSTEM + offset];
		brand = args[Constants.INDEX_BRAND + offset];
		netType = args[Constants.INDEX_NETTYPE + offset];
		country = args[Constants.INDEX_COUNTRY + offset];
		province = args[Constants.INDEX_PROVINCE + offset];
		operators = args[Constants.INDEX_OPERATORS + offset];
	}

	public String[] toStringArray() {
		return new String[] { channel, accountType, gender, age, gameServer,
				resolution, operSystem, brand, netType, country, province,
				operators };
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
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

	public String getGameServer() {
		return gameServer;
	}

	public void setGameServer(String gameServer) {
		this.gameServer = gameServer;
	}

	public String getResolution() {
		return resolution;
	}

	public void setResolution(String resolution) {
		this.resolution = resolution;
	}

	public String getOperSystem() {
		return operSystem;
	}

	public void setOperSystem(String operSystem) {
		this.operSystem = operSystem;
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

	@Override
	public String toString() {
		return "CommonExtend [channel=" + channel + ", accountType="
				+ accountType + ", gender=" + gender + ", age=" + age
				+ ", gameServer=" + gameServer + ", resolution=" + resolution
				+ ", operSystem=" + operSystem + ", brand=" + brand
				+ ", netType=" + netType + ", country=" + country
				+ ", province=" + province + ", operators=" + operators + "]";
	}

}
