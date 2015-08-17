package net.digitcube.hadoop.mapreduce.domain;

import net.digitcube.hadoop.util.StringUtil;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月17日 下午7:47:16 @copyrigt www.digitcube.net
 */

public class UserInfoLog implements MapReduceVO {

	public final static int INDEX_ActTime = 17;
	public final static int INDEX_RegTime = 18;

	private int actTime;
	private int regTime;

	public UserInfoLog() {
	}

	public UserInfoLog(String[] args) {
		actTime = StringUtil.convertInt(args[INDEX_ActTime], 0);
		regTime = StringUtil.convertInt(args[INDEX_RegTime], 0);
	}

	@Override
	public String[] toStringArray() {
		return new String[] { actTime + "", regTime + "" };
	}

	public int getActTime() {
		return actTime;
	}

	public void setActTime(int actTime) {
		this.actTime = actTime;
	}

	public int getRegTime() {
		return regTime;
	}

	public void setRegTime(int regTime) {
		this.regTime = regTime;
	}

}
