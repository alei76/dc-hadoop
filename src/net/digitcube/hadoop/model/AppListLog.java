package net.digitcube.hadoop.model;

import net.digitcube.hadoop.util.StringUtil;

/**
 * 应用列表
 * 
 * @author Ivan <br>
 * @date 2014-6-10 <br>
 * @version 1.0 <br>
 */
public class AppListLog extends HeaderLog {

	private String packageName;
	private String appName;
	private String appVer;
	private String installTime;
	private String lastAccessTime;
	private int setUpTimes;
	private int upTime;
	private String ext;

	public String getPackageName() {
		return packageName;
	}

	public void setPackageName(String packageName) {
		this.packageName = packageName;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getAppVer() {
		return appVer;
	}

	public void setAppVer(String appVer) {
		this.appVer = appVer;
	}

	public String getInstallTime() {
		return installTime;
	}

	public void setInstallTime(String installTime) {
		this.installTime = installTime;
	}

	public String getLastAccessTime() {
		return lastAccessTime;
	}

	public void setLastAccessTime(String lastAccessTime) {
		this.lastAccessTime = lastAccessTime;
	}

	public int getSetUpTimes() {
		return setUpTimes;
	}

	public void setSetUpTimes(int setUpTimes) {
		this.setUpTimes = setUpTimes;
	}

	public int getUpTime() {
		return upTime;
	}

	public void setUpTime(int upTime) {
		this.upTime = upTime;
	}

	public String getExt() {
		return ext;
	}

	public void setExt(String ext) {
		this.ext = ext;
	}

	public AppListLog(String[] fields) {
		super(fields);
		int length = fields.length;
		packageName = fields[length - 8];
		appName = fields[length - 7];
		appVer = fields[length - 6];
		installTime = fields[length - 5];
		lastAccessTime = fields[length - 4];
		setUpTimes = StringUtil.convertInt(fields[length - 3], 0);
		upTime = StringUtil.convertInt(fields[length - 2], 0);
		ext = fields[length - 1];
	}

	public void setFields(String[] fields) {
		super.setFields(fields);
		int length = fields.length;
		packageName = fields[length - 8];
		appName = fields[length - 7];
		appVer = fields[length - 6];
		installTime = fields[length - 5];
		lastAccessTime = fields[length - 4];
		setUpTimes = StringUtil.convertInt(fields[length - 3], 0);
		upTime = StringUtil.convertInt(fields[length - 2], 0);
		ext = fields[length - 1];
	}

	/**
	 * 该方法为临时解决问题用
	 * 
	 * @return
	 */
	public String[] toOldVersionArr() {
		String[] arr = new String[20];
		arr[0] = this.getTimestamp();
		arr[1] = this.getAppID();
		arr[2] = this.getUID();
		arr[3] = this.getAccountID();
		arr[4] = this.getPlatform();
		arr[5] = this.getChannel();
		arr[6] = this.getAccountType();
		arr[7] = this.getGender();
		arr[8] = this.getAge();
		arr[9] = this.getGameServer();
		arr[10] = this.getResolution();
		arr[11] = this.getOperSystem();
		arr[12] = this.getBrand();
		arr[13] = this.getNetType();
		arr[14] = this.getCountry();
		arr[15] = this.getProvince();
		arr[16] = this.getOperators();
		arr[17] = this.packageName;
		arr[18] = this.appName;
		arr[19] = this.appVer;
		arr[20] = this.installTime;
		arr[21] = this.lastAccessTime;
		arr[22] = this.setUpTimes + "";
		arr[23] = this.upTime + "";
		arr[24] = this.ext;
		return arr;
	}
}
