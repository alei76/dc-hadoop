package net.digitcube.hadoop.mapreduce.warehouse;

import net.digitcube.hadoop.util.StringUtil;

public class AppModel {

	private String appId;

	private String appName;

	private int typeId;

	private String typeName;

	private String currency;

	private String flag;

	private int status;

	private int companyId;

	private String companyName;

	private int createTime;

	public AppModel() {
	}

	public AppModel(String appId, String appName, String companyName) {
		this.appId = appId;
		this.appName = appName;
		this.companyName = companyName;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public int getTypeId() {
		return typeId;
	}

	public void setTypeId(int typeId) {
		this.typeId = typeId;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public int getCompanyId() {
		return companyId;
	}

	public void setCompanyId(int companyId) {
		this.companyId = companyId;
	}

	public String getCompanyName() {
		return companyName;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}

	public int getCreateTime() {
		return createTime;
	}

	public void setCreateTime(int createTime) {
		this.createTime = createTime;
	}

	@Override
	public String toString() {
		return appId + "\t" + appName + "\t" + typeId + "\t" + typeName + "\t" + currency + "\t" + flag + "\t" + status
				+ "\t" + companyId + "\t" + companyName + "\t" + createTime + "\t"
				+ StringUtil.date_yyyyMMddHHmmss(createTime * 1000L);
	}
}
