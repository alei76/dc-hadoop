package net.digitcube.hadoop.mapreduce.domain;

import net.digitcube.hadoop.util.StringUtil;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月17日 下午7:52:52 @copyrigt www.digitcube.net
 */

public class OnlineLog implements MapReduceVO {

	private int loginTime;
	private int onlineTime;
	private int level;

	public final static int INDEX_LoginTime = 17;
	public final static int INDEX_OnlineTime = 18;
	public final static int INDEX_Level = 19;

	public OnlineLog() {
	}

	public OnlineLog(String[] args) {
		loginTime = StringUtil.convertInt(args[INDEX_LoginTime], 0);
		onlineTime = StringUtil.convertInt(args[INDEX_OnlineTime], 0);
		level = StringUtil.convertInt(args[INDEX_Level], 0);
	}

	@Override
	public String[] toStringArray() {
		return new String[] { loginTime + "", onlineTime + "", level + "" };
	}

	public int getLoginTime() {
		return loginTime;
	}

	public void setLoginTime(int loginTime) {
		this.loginTime = loginTime;
	}

	public int getOnlineTime() {
		return onlineTime;
	}

	public void setOnlineTime(int onlineTime) {
		this.onlineTime = onlineTime;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

}
