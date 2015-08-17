package net.digitcube.hadoop.model.channel;

import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.util.StringUtil;

import org.apache.commons.lang.StringUtils;

/**
 * 新的公共头部
 * 
 * @author sam.xie
 * @date 2015年3月3日 下午2:24:09
 * @version 1.0
 */
public class HeaderLog2 {
	/** 时间戳 */
	private String timestamp;
	/** sdk版本号 */
	private String sdkVersion;
	/** APP唯一编号 */
	private String appID;
	/** 平台 */
	private String platform;
	/** APP版本 */
	private String appVersion;
	/** 唯一设备号 */
	private String UID;
	/** 渠道 */
	private String channel;
	/** 本次上报IP地址 */
	private String IP;
	/** 扩展字段，为Json格式字符串 */
	private String extend;
	/** 扩展字段的Map集合 */
	private Map<String, String> extendMap;
	
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

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getAppVersion() {
		return appVersion;
	}

	public void setAppVersion(String appVersion) {
		this.appVersion = appVersion;
	}

	public String getUID() {
		return UID;
	}

	public void setUID(String uID) {
		UID = uID;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getIP() {
		return IP;
	}

	public void setIP(String iP) {
		IP = iP;
	}

	public String getExtend() {
		return extend;
	}

	public void setExtend(String extend) {
		this.extend = extend;
	}

	public void setExtendMap(Map<String, String> extendMap) {
		this.extendMap = extendMap;
	}

	/** 获取扩展字段中指定的参数值，不存在，就以 - 代替 */
	public String getExtendValue(String key) {
		String value = getExtendMap().get(key);
		if(StringUtils.isBlank(value))
			return "-";
		return value;
	}

	public HeaderLog2() {
	}
	
	public HeaderLog2(String[] fields) {
		setFields(fields);
	}

	public void setFields(String[] fields) {
		this.timestamp = fields[Index.TIMESTAMP];
		this.sdkVersion = fields[Index.SDKVERSION];
		this.appID = fields[Index.APPID];
		this.platform = fields[Index.PLATFORM];
		this.appVersion = fields[Index.APPVERSION];
		this.UID = fields[Index.UID];
		this.channel = fields[Index.CHANNEL];
		this.IP = fields[Index.IP];
		this.extend = fields[Index.EXTEND];

	}

	public String[] toStringArr() {
		return new String[] { timestamp, sdkVersion, appID, platform, appVersion, UID, channel, IP, extend };
	}

	/** 把 json 格式的的 extend 解析为 map */
	private Map<String, String> getExtendMap() {
		if (null == extendMap) {
			extendMap = StringUtil.getMapFromJson(extend);
		}
		if (null == extendMap) {
			extendMap = new HashMap<String, String>();
		}
		return extendMap;
	}

	public class Index {
		public static final int TIMESTAMP = 0;
		public static final int SDKVERSION = 1;
		public static final int APPID = 2;
		public static final int PLATFORM = 3;
		public static final int APPVERSION = 4;
		public static final int UID = 5;
		public static final int CHANNEL = 6;
		public static final int IP = 7;
		public static final int EXTEND = 8;

	}

	/** 扩展字段中的参数常量 */
	public class ExtendKey {
		public static final String MAC = "mac";
		public static final String IMEI = "imei";
		public static final String IMSI = "imsi";
		public static final String IDFA = "idfa";
		public static final String SCREEN = "screen";
		public static final String OS = "os";
		public static final String BRAND = "brand";
		public static final String LANG = "lang";
		public static final String TIMEZONE = "timeZone";
		public static final String LON = "lon";
		public static final String LAT = "lat";
		public static final String NETWORK = "network";
		public static final String MOBILEOP = "mobileOp";
		public static final String CITY = "city";
		public static final String PROV = "prov";
		public static final String CNTY = "cnty";
		public static final String NETOP = "netOp";
	}
}
