package net.digitcube.hadoop.model.warehouse;

import com.google.gson.annotations.Expose;

public class AppInfo {
	
	@Expose
	private String pkgName;
	
	@Expose
	private String appName;
	
	@Expose
	private String appVersion;

	public AppInfo(){}
	
	public AppInfo(String pkgName,String appName){
		this.pkgName = pkgName;
		this.appName = appName;
	}
	
	public String getPkgName() {
		return pkgName;
	}

	public void setPkgName(String pkgName) {
		this.pkgName = pkgName;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getAppVersion() {
		return appVersion;
	}

	public void setAppVersion(String appVersion) {
		this.appVersion = appVersion;
	}
	
}
