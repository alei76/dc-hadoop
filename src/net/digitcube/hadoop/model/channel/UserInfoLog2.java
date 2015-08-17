package net.digitcube.hadoop.model.channel;

import net.digitcube.hadoop.util.StringUtil;

public class UserInfoLog2 extends HeaderLog2 {

	private int actTime = 0;
	
	public int getActTime() {
		return actTime;
	}

	public void setActTime(int actTime) {
		this.actTime = actTime;
	}

	public UserInfoLog2(String[] fields) {
		super(fields);
		int length = fields.length;
		this.actTime = StringUtil.convertInt(fields[length - 1], 0);
	}
}
