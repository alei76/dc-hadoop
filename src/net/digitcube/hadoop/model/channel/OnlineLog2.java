package net.digitcube.hadoop.model.channel;

import net.digitcube.hadoop.util.StringUtil;

public class OnlineLog2 extends HeaderLog2 {

	private int loginTime = 0;
	private int onlineTime = 0;
	
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


	public OnlineLog2(String[] fields) {
		super(fields);
		int length = fields.length;
		this.loginTime = StringUtil.convertInt(fields[length - 2], 0);
		this.onlineTime = StringUtil.convertInt(fields[length - 1], 0);
	}
}
