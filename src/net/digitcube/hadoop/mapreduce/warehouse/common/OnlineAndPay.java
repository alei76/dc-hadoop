package net.digitcube.hadoop.mapreduce.warehouse.common;

import net.digitcube.hadoop.util.StringUtil;

public class OnlineAndPay {

	private String[] keys;

	private int lastUpdateTime;

	private int firstLoginTime;
	private int lastLoginTime;
	private long loginTimes;
	private long duration;
	private int loginDays;

	private int firstPayTime;
	private int lastPayTime;
	private long payTimes;
	private float payAmount;
	private int payDays;

	public OnlineAndPay() {
	}

	public OnlineAndPay(String[] keys) {
		this.keys = keys;
	}

	public OnlineAndPay(int keyOffset, String[] paramArr) {
		setParams(keyOffset, paramArr);
	}

	public void setParams(int keyOffset, String[] paramArr) {
		keys = new String[keyOffset];
		for (int i = 0; i < keyOffset; i++) {
			keys[i] = paramArr[i];
		}

		this.lastUpdateTime = StringUtil.convertInt(paramArr[keyOffset++], 0);

		this.firstLoginTime = StringUtil.convertInt(paramArr[keyOffset++], 0);
		this.lastLoginTime = StringUtil.convertInt(paramArr[keyOffset++], 0);
		this.loginTimes = StringUtil.convertLong(paramArr[keyOffset++], 0);
		this.duration = StringUtil.convertLong(paramArr[keyOffset++], 0);
		this.loginDays = StringUtil.convertInt(paramArr[keyOffset++], 0);

		this.firstPayTime = StringUtil.convertInt(paramArr[keyOffset++], 0);
		this.lastPayTime = StringUtil.convertInt(paramArr[keyOffset++], 0);
		this.payTimes = StringUtil.convertLong(paramArr[keyOffset++], 0);
		this.payAmount = StringUtil.convertFloat(paramArr[keyOffset++], 0);
		this.payDays = StringUtil.convertInt(paramArr[keyOffset++], 0);
	}

	public void mergeParams(int keyOffset, String[] paramArr) {
		setLastUpdateTime(StringUtil.convertInt(paramArr[keyOffset++], 0));

		setFirstLoginTime(StringUtil.convertInt(paramArr[keyOffset++], 0));
		setLastLoginTime(StringUtil.convertInt(paramArr[keyOffset++], 0));
		addLoginTimes(StringUtil.convertLong(paramArr[keyOffset++], 0));
		addDuration(StringUtil.convertLong(paramArr[keyOffset++], 0));
		addLoginDays(StringUtil.convertInt(paramArr[keyOffset++], 0));

		setFirstPayTime(StringUtil.convertInt(paramArr[keyOffset++], 0));
		setLastPayTime(StringUtil.convertInt(paramArr[keyOffset++], 0));
		addPayTimes(StringUtil.convertLong(paramArr[keyOffset++], 0));
		addPayAmount(StringUtil.convertFloat(paramArr[keyOffset++], 0));
		addPayDays(StringUtil.convertInt(paramArr[keyOffset++], 0));
	}

	public void mergeDay(OnlineAndPay dayStat) {
		this.setLastUpdateTime(dayStat.getLastUpdateTime());

		this.setFirstLoginTime(dayStat.getFirstLoginTime());
		this.setLastLoginTime(dayStat.getLastLoginTime());
		this.addLoginTimes(dayStat.getLoginTimes());
		this.addDuration(dayStat.getDuration());
		this.addLoginDays(dayStat.getLoginDays());

		this.setFirstPayTime(dayStat.getFirstPayTime());
		this.setLastPayTime(dayStat.getLastPayTime());
		this.addPayTimes(dayStat.getPayTimes());
		this.addPayAmount(dayStat.getPayAmount());
		this.addPayDays(dayStat.getPayDays());
	}

	public String[] getKeys() {
		return keys;
	}

	public void setKeys(String[] keys) {
		this.keys = keys;
	}

	public int getLastUpdateTime() {
		return lastUpdateTime;
	}

	public void setLastUpdateTime(int lastUpdateTime) {
		if (this.lastUpdateTime < lastUpdateTime || this.lastUpdateTime <= 0) {
			this.lastUpdateTime = lastUpdateTime;
		}
	}

	public int getFirstLoginTime() {
		return firstLoginTime;
	}

	public void setFirstLoginTime(int firstLoginTime) {
		if ((this.firstLoginTime > firstLoginTime && firstLoginTime > 0) || this.firstLoginTime <= 0) {
			this.firstLoginTime = firstLoginTime;
		}
	}

	public int getFirstPayTime() {
		return firstPayTime;
	}

	public void setFirstPayTime(int firstPayTime) {
		if ((this.firstPayTime > firstPayTime && firstPayTime > 0) || this.firstPayTime <= 0) {
			this.firstPayTime = firstPayTime;
		}
	}

	public int getLastLoginTime() {
		return lastLoginTime;
	}

	public void setLastLoginTime(int lastLoginTime) {
		if (this.lastLoginTime < lastLoginTime || this.lastLoginTime <= 0) {
			this.lastLoginTime = lastLoginTime;
		}
	}

	public int getLastPayTime() {
		return lastPayTime;
	}

	public void setLastPayTime(int lastPayTime) {
		if (this.lastPayTime < lastPayTime || this.lastPayTime <= 0) {
			this.lastPayTime = lastPayTime;
		}
	}

	public long getLoginTimes() {
		return loginTimes;
	}

	public void setLoginTimes(long loginTimes) {
		this.loginTimes = loginTimes;
	}

	public void addLoginTimes(long loginTimes) {
		this.loginTimes += loginTimes;
	}

	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	public void addDuration(long duration) {
		this.duration += duration;
	}

	public long getPayTimes() {
		return payTimes;
	}

	public void setPayTimes(long payTimes) {
		this.payTimes = payTimes;
	}

	public void addPayTimes(long payTimes) {
		this.payTimes += payTimes;
	}

	public float getPayAmount() {
		return payAmount;
	}

	public void setPayAmount(float payAmount) {
		this.payAmount = payAmount;
	}

	public void addPayAmount(float payAmount) {
		this.payAmount += payAmount;
	}

	public int getLoginDays() {
		return loginDays;
	}

	public void setLoginDays(int loginDays) {
		this.loginDays = loginDays;
	}

	public void addLoginDays(int loginDays) {
		this.loginDays += loginDays;
	}

	public int getPayDays() {
		return payDays;
	}

	public void setPayDays(int payDays) {
		this.payDays = payDays;
	}

	public void addPayDays(int payDays) {
		this.payDays += payDays;
	}

	public void clear() {
		keys = null;
		lastUpdateTime = 0;

		firstLoginTime = 0;
		lastLoginTime = 0;
		loginTimes = 0;
		duration = 0;
		loginDays = 0;

		firstPayTime = 0;
		lastPayTime = 0;
		payTimes = 0;
		payAmount = 0;
		payDays = 0;
	}

	public String toSimpleString() {
		return loginTimes + ":" + duration + ":" + payTimes + ":" + payAmount;
	}

	public String[] toArray() {
		return new String[] { lastUpdateTime + "", firstLoginTime + "", lastLoginTime + "", loginTimes + "",
				duration + "", loginDays + "", firstPayTime + "", lastPayTime + "", payTimes + "", payAmount + "",
				payDays + "" };
	}

}
