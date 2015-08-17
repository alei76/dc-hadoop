package net.digitcube.hadoop.mapreduce.channel;
import java.util.Map;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.util.StringUtil;

public class ExtendFieldsHelper {

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
	
	private String extnedFields;
	private Map<String, String> extendMap;
	
	public String getExtnedFields() {
		return extnedFields;
	}

	public void setExtnedFields(String extnedFields) {
		this.extnedFields = extnedFields;
	}

	public Map<String, String> getExtendMap() {
		return extendMap;
	}

	public void setExtendMap(Map<String, String> extendMap) {
		this.extendMap = extendMap;
	}

	public ExtendFieldsHelper(String extnedFields){
		this.extnedFields = extnedFields;
		this.extendMap = StringUtil.getMapFromJson(extnedFields);
	}
	
	
	public String getMac() {
		return getExtendMap().get(MAC);
	}

	public String getImei() {
		return getExtendMap().get(IMEI);
	}

	public String getImsi() {
		return getExtendMap().get(IMSI);
	}

	public String getIdfa() {
		return getExtendMap().get(IDFA);
	}

	public String getScreen() {
		return getExtendMap().get(SCREEN);
	}

	public String getOs() {
		return getExtendMap().get(OS);
	}

	public String getBrand() {
		String brand = getExtendMap().get(BRAND);
		if(null == brand){
			return MRConstants.INVALID_PLACE_HOLDER_CHAR; 
		}
		return brand;
	}

	public String getLang() {
		return getExtendMap().get(LANG);
	}

	public String getTimezone() {
		return getExtendMap().get(TIMEZONE);
	}

	public String getLon() {
		return getExtendMap().get(LON);
	}

	public String getLat() {
		return getExtendMap().get(LAT);
	}

	public String getNetwork() {
		return getExtendMap().get(NETWORK);
	}

	public String getMobileop() {
		return getExtendMap().get(MOBILEOP);
	}

	public String getCity() {
		return getExtendMap().get(CITY);
	}

	public String getProv() {
		String province = getExtendMap().get(PROV);
		if(null == province){
			return MRConstants.INVALID_PLACE_HOLDER_CHAR; 
		}
		return province;
	}

	public String getCnty() {
		String country = getExtendMap().get(CNTY);
		if(null == country){
			return MRConstants.INVALID_PLACE_HOLDER_CHAR; 
		}
		return country;
	}

	public String getNetop() {
		return getExtendMap().get(NETOP);
	}

	public String getFieldsByKey(String key, String defaultVal){
		return null == getExtendMap().get(key) ? defaultVal : getExtendMap().get(key);
	}
}
