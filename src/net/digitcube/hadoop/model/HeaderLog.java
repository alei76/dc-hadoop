package net.digitcube.hadoop.model;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.util.StringUtil;

public class HeaderLog {

	private String timestamp;
	private String sdkVersion;
	private String appID;
	private String UID;
	private String accountID;
	private String platform;
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
	private String extend_1;
	private String extend_2;
	private String extend_3;
	private String extend_4;
	private String extend_5;

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getSdkVersion() {
		return sdkVersion;
	}

	public void setSdkVersion(String sdkVersion) {
		this.sdkVersion = sdkVersion;
	}

	public String getAppID() {
		return appID;
	}

	public void setAppID(String appID) {
		this.appID = appID;
	}

	public String getUID() {
		return UID;
	}

	public void setUID(String uID) {
		UID = uID;
	}

	public String getAccountID() {
		return accountID;
	}

	public void setAccountID(String accountID) {
		this.accountID = accountID;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
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

	public String getExtend_1() {
		return extend_1;
	}

	public void setExtend_1(String extend_1) {
		this.extend_1 = extend_1;
	}

	public String getExtend_2() {
		return extend_2;
	}

	public void setExtend_2(String extend_2) {
		this.extend_2 = extend_2;
	}

	public String getExtend_3() {
		return extend_3;
	}

	public void setExtend_3(String extend_3) {
		this.extend_3 = extend_3;
	}

	public String getExtend_4() {
		return extend_4;
	}

	public void setExtend_4(String extend_4) {
		this.extend_4 = extend_4;
	}

	public String getExtend_5() {
		return extend_5;
	}

	public void setExtend_5(String extend_5) {
		this.extend_5 = extend_5;
	}

	public HeaderLog(String[] fields) {
		setFields(fields);
	}

	public void setFields(String[] fields) {
		// 20141003
		// 后台版 sdk 上报的版本号中带有字母和小数点
		// 导致协议解析出错，这些版本如下：
		// 1.0.0
		// 1.0
		// 1.0.1
		// java_1.0.0
		// java_1.0.1
		// cpp_1.0.0
		// cpp_1.0.1
		String versionStr = fields[1];
		versionStr = versionStr.replace("java_", "").replace("cpp_", "").replace(".", "");
		// 与张耀东确认，把这些版本替换掉
		// SDK 上报数据
		// 旧版本第二个字段为 appID|appVersion 是非数字的字符串
		// 新版本第二个字段为 sdkVersion 是一个非 0 的数字
		int sdkVersion = StringUtil.convertInt(versionStr, 0);
		boolean isNewSDKVersion = 0 == sdkVersion ? false : true;

		if (!isNewSDKVersion) {
			this.timestamp = fields[0];
			// this.sdkVersion = fields[1];
			this.appID = fields[1];
			this.UID = fields[2];
			this.accountID = fields[3];
			this.platform = fields[4];
			this.channel = fields[5];
			this.accountType = fields[6];
			this.gender = fields[7];
			this.age = fields[8];
			this.gameServer = fields[9];
			this.resolution = fields[10];
			this.operSystem = fields[11];
			this.brand = fields[12];
			this.netType = fields[13];
			this.country = fields[14];
			this.province = fields[15];
			this.operators = fields[16];
			/*
			 * this.extend_1 = fields[18]; this.extend_2 = fields[19]; this.extend_3 = fields[20]; this.extend_4 =
			 * fields[21]; this.extend_5 = fields[22];
			 */
		} else {
			this.timestamp = fields[0];
			this.sdkVersion = fields[1];
			this.appID = fields[2];
			this.UID = fields[3];
			this.accountID = fields[4];
			this.platform = fields[5];
			this.channel = fields[6];
			this.accountType = fields[7];
			this.gender = fields[8];
			this.age = fields[9];
			this.gameServer = fields[10];
			this.resolution = fields[11];
			this.operSystem = fields[12];
			this.brand = fields[13];
			this.netType = fields[14];
			this.country = fields[15];
			this.province = fields[16];
			this.operators = fields[17];
			this.extend_1 = fields[18];
			this.extend_2 = fields[19];
			this.extend_3 = fields[20];
			this.extend_4 = fields[21];
			this.extend_5 = fields[22];
		}
	}

	public String[] toStringArr() {
		return new String[] { timestamp, sdkVersion, appID, UID, accountID, platform, channel, accountType, gender,
				age, gameServer, resolution, operSystem, brand, netType, country, province, operators, extend_1,
				extend_2, extend_3, extend_4, extend_5 };
	}

	private Map<String, String> ext5Map;

	/** 把 json 对象的 ext5 解析为 map */
	public Map<String, String> getExt5Map() {
		if (null == ext5Map) {
			ext5Map = StringUtil.getMapFromJson(extend_5);
		}
		if (null == ext5Map) {
			ext5Map = new HashMap<String, String>();
		}
		return ext5Map;
	}

	public String getH5PromotionApp() {
		String promotionApp = getExt5Map().get(Constants.H5_APP);
		if (StringUtil.isEmpty(promotionApp)) {
			promotionApp = MRConstants.INVALID_PLACE_HOLDER_CHAR;
			getExt5Map().put(Constants.H5_APP, promotionApp);
		}
		return promotionApp;
	}

	public String getH5Domain() {
		String domain = getExt5Map().get(Constants.H5_DOMAIN);
		if (StringUtil.isEmpty(domain)) {
			domain = MRConstants.INVALID_PLACE_HOLDER_CHAR;
			getExt5Map().put(Constants.H5_DOMAIN, domain);
		}
		return domain;
	}

	public String getH5Refer() {
		String refer = getExt5Map().get(Constants.H5_REF);
		if (StringUtil.isEmpty(refer)) {
			refer = MRConstants.INVALID_PLACE_HOLDER_CHAR;
			getExt5Map().put(Constants.H5_REF, refer);
		}
		return refer;
	}

	public String getH5ParentAccId() {
		return getExt5Map().get(Constants.H5_PARENT);
	}

	public String getH5CRtTime() {
		return getExt5Map().get(Constants.H5_CRTIME);
	}

	/** 从扩展字段 ext5 中解析得到 h5 玩家的创建时间，与结算时间相等则为新增玩家 */
	/** 已经废弃 */
	@Deprecated
	public boolean isH5NewAddPlayer(int statDate) {
		String createTime = getExt5Map().get(Constants.H5_CRTIME);
		if (StringUtil.isEmpty(createTime)) {
			return false;
		}

		int crTime = StringUtil.convertInt(createTime, 0);
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(1000L * crTime);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return statDate == (int) (cal.getTimeInMillis() / 1000);
	}

	public boolean isReferDomainEmpty() {
		String promoptApp = getH5PromotionApp();
		String domain = getH5Domain();
		String refer = getH5Refer();
		if (StringUtil.isEmpty(promoptApp) || StringUtil.isEmpty(domain) || StringUtil.isEmpty(refer)) {
			return true;
		}
		return false;
	}

	// sim卡扩展字段
	// sim卡运营商号
	public String getSimOpCode() {
		// 20141027
		// SDK 中 SIM 卡运营商码安卓和 iOS 两个版本字段写反了
		// 正确的值应该为 Constants.SIM_OPERATOR
		// iOS SDK 20141027 之前发出去的版本把值填到了 Constants.SIM_OPERATOR_ISO 中
		// 这里需对 iOS 历史版本兼容
		if (MRConstants.PLATFORM_iOS_STR.equals(this.getPlatform())) {
			// 优先取 SIM_OPERATOR
			String simCarkOpCode = getExt5Map().get(Constants.SIM_OPERATOR);
			if (isNumber(simCarkOpCode)) {
				return simCarkOpCode;
			}
			simCarkOpCode = getExt5Map().get(Constants.SIM_OPERATOR_ISO);
			if (isNumber(simCarkOpCode)) {
				return simCarkOpCode;
			}
			return null;
		} else {
			String simCarkOpCode = getExt5Map().get(Constants.SIM_OPERATOR);
			if (isNumber(simCarkOpCode)) {
				return simCarkOpCode;
			}
			return null;
		}
	}

	// 手机支持多卡时，simCarkOpCode 每个码以逗号分开，如:46002,46003
	private boolean isNumber(String simCarkOpCode) {
		try {
			if (null == simCarkOpCode || "".equals(simCarkOpCode.trim()) || ",".equals(simCarkOpCode.trim())) {
				return false;
			}

			String[] codes = simCarkOpCode.split(",");
			for (String code : codes) {
				Integer.valueOf(code.trim());
			}
			// 全部都能解析则说明正确
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	public String getMac() {
		return getExt5Map().get(Constants.WIFIMAC);
	}

	public String getImei() {
		return getExt5Map().get(Constants.IMEI);
	}
	
	public String getImsi() {
		return getExt5Map().get(Constants.IMSI);
	}

	public String getRoleID() {
		return getExt5Map().get(Constants.ROLEID);
	}

	public String getRoleName() {
		return getExt5Map().get(Constants.ROLENAME);
	}

	public String getIdfa() {
		String idfa = getExt5Map().get(Constants.IDFA);
		if (StringUtil.isEmpty(idfa)) {
			// IOS 7,8 UID就是IDFA
			if (MRConstants.PLATFORM_iOS_STR.equals(this.getPlatform()) && !StringUtil.isEmpty(this.getOperSystem())
					&& (this.getOperSystem().indexOf("7") == 0 || this.getOperSystem().indexOf("8") == 0)) {
				idfa = UID;
			}
		}
		return idfa;
	}
	// public String getInvokeLog(){
	// return getExt5Map().get(Constants.INVOKELOG);
	// }
	//
	// public String getSimOperatorIso(){
	// return getExt5Map().get(Constants.SIM_OPERATOR_ISO);
	// }
	//
	// public String getSimOperator(){
	// return getExt5Map().get(Constants.SIM_OPERATOR);
	// }
	//
	// public String getImei(){
	// return getExt5Map().get(Constants.IMEI);
	// }
	//
	// public String getRePortMode(){
	// return getExt5Map().get(Constants.REPORTMODE);
	// }
	//
	// public String getWiFiMac(){
	// return getExt5Map().get(Constants.WIFIMAC);
	// }

}
