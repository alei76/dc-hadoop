package net.digitcube.hadoop.mapreduce.warehouse.common;

import net.digitcube.hadoop.util.StringUtil;

public class OnlineAndPayRolling extends OnlineAndPay {

	private int loginMark;
	private String loginRecord;
	private int payMark;	
	private String payRecord;

	public OnlineAndPayRolling() {
	}

	public OnlineAndPayRolling(String[] keys) {
		super(keys);
	}

	public OnlineAndPayRolling(int keyOffset, String[] paramArr) {
		this.setParams(keyOffset, paramArr);
	}

	public void setParams(int keyOffset, String[] paramArr) {
		super.setParams(keyOffset, paramArr);
		if (paramArr.length - keyOffset > 11) {
			this.setLoginMark(StringUtil.convertInt(paramArr[keyOffset + 11 + 0], 0));
			this.setLoginRecord(paramArr[keyOffset + 11 + 1]);
			this.setPayMark(StringUtil.convertInt(paramArr[keyOffset + 11 + 2], 0));
			this.setPayRecord(paramArr[keyOffset + 11 + 3]);
		}
	}

	// 对于两个时间段没有重合的滚存，可以直接累加或更新
	public void mergeRolling(int keyOffset, String[] paramArr) {
		this.setLastUpdateTime(StringUtil.convertInt(paramArr[keyOffset++], 0));

		this.setFirstLoginTime(StringUtil.convertInt(paramArr[keyOffset++], 0));
		this.setLastLoginTime(StringUtil.convertInt(paramArr[keyOffset++], 0));
		this.addLoginTimes(StringUtil.convertInt(paramArr[keyOffset++], 0));
		this.addDuration(StringUtil.convertInt(paramArr[keyOffset++], 0));
		this.addLoginDays(StringUtil.convertInt(paramArr[keyOffset++], 0));

		this.setFirstPayTime(StringUtil.convertInt(paramArr[keyOffset++], 0));
		this.setLastPayTime(StringUtil.convertInt(paramArr[keyOffset++], 0));
		this.addPayTimes(StringUtil.convertInt(paramArr[keyOffset++], 0));
		this.addPayAmount(StringUtil.convertInt(paramArr[keyOffset++], 0));
		this.addPayDays(StringUtil.convertInt(paramArr[keyOffset++], 0));
	}

	public int getLoginMark() {
		return loginMark;
	}

	public void setLoginMark(int loginMark) {
		this.loginMark = loginMark;
	}

	public String getLoginRecord() {
		return loginRecord;
	}

	public void setLoginRecord(String loginRecord) {
		this.loginRecord = loginRecord;
	}

	public int getPayMark() {
		return payMark;
	}

	public void setPayMark(int payMark) {
		this.payMark = payMark;
	}

	public String getPayRecord() {
		return payRecord;
	}

	public void setPayRecord(String payRecord) {
		this.payRecord = payRecord;
	}

	public void clear() {
		super.clear();
		this.loginMark = 0;
		this.loginRecord = "";
		this.payMark = 0;
		this.payRecord = "";
	}

	public String[] toArray() {
		return new String[] { getLastUpdateTime() + "", getFirstLoginTime() + "", getLastLoginTime() + "",
				getLoginTimes() + "", getDuration() + "", getLoginDays() + "", getFirstPayTime() + "",
				getLastPayTime() + "", getPayTimes() + "", getPayAmount() + "", getPayDays() + "" ,
				getLoginMark() + "", getLoginRecord() + "", getPayMark() + "", getPayRecord() + "" };
	}

}
