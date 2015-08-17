package net.digitcube.hadoop.model.channel;

import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.util.StringUtil;

/**
 * 新版的事件文件格式
 * 
 * @author sam.xie
 * @date 2015年3月4日 下午8:54:02
 * @version 1.0
 */
public class EventLog2 extends HeaderLog2 {

	/** 自定义事件名 */
	private String eventId;

	/** 开始时间，秒时间戳 */
	private String startTime;

	/** 结束时间，秒时间戳 */
	private String endTime;

	/** 持续时长 ，秒 */
	private String duration;

	/** 自定义事件详情，Json格式 */
	private String eventAttr;

	/** eventAttr的Json字符串转换为 Map对象 */
	private Map<String, String> attrMap;

	/** 事件日志最小的参数数量 */
	private static final int PARAM_NUM = 14; // 事件日志的最小参数数量（包括头字段）

	private static final int HEADER_NUM = 9; // 头字段包括9个字段

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}

	public String getEventAttr() {
		return eventAttr;
	}

	public void setEventAttr(String eventAttr) {
		this.eventAttr = eventAttr;
	}

	public Map<String, String> getAttrMap() {
		if (null == attrMap) {
			setAttrMap(eventAttr);
		}
		if (null == attrMap) {
			attrMap = new HashMap<String, String>();
		}
		return attrMap;
	}

	/**
	 * <pre>
	 * 将Json格式字符串转换为Map对象
	 * 这里只解析如{"age":123,"name":"sam"}这类简单的Json串
	 * 暂不支持JSON为列表，对象的情况
	 * @author sam.xie
	 * @date 2015年3月2日 下午8:26:05
	 */
	public void setAttrMap(String attrJson) {
		this.attrMap = StringUtil.getMapFromJson(attrJson);
	}

	public EventLog2(String[] fields) {
		super(fields);
		if (fields.length < PARAM_NUM) {
			throw new RuntimeException("Invalid event, fields is less than +" + PARAM_NUM);
		}
		this.eventId = fields[HEADER_NUM + Index.EVENTID];
		this.startTime = fields[HEADER_NUM + Index.STARTTIME];
		this.endTime = fields[HEADER_NUM + Index.ENDTIME];
		this.duration = fields[HEADER_NUM + Index.DURATION];
		this.eventAttr = fields[HEADER_NUM + Index.EVENTATTR];

		/*if (fields[HeaderLog2.Index.APPID].length() != 32
				|| !(eventId.startsWith("DESelf") || eventId.startsWith("_DESelf"))) {
			// 第3个字段为AppID，必须是32位
			// 第9个字段为自定事件，必须以"DESelf"或"_DESelf开头"
			throw new RuntimeException("Invalid event format!");
		}*/
	}

	public void setFields(String[] fields) {
		super.setFields(fields);
		this.eventId = fields[HEADER_NUM];
		this.startTime = fields[HEADER_NUM + 1];
		this.endTime = fields[HEADER_NUM + 2];
		this.duration = fields[HEADER_NUM + 3];
		this.eventAttr = fields[HEADER_NUM + 4];
	}

	/** 事件属性中的参数常量 */
	public class AttrKey {
		public static final String TS = "ts";
		public static final String RESPAIRS = "resPairs";
		public static final String RLIDS = "rlIds";
		public static final String RESIDS = "resIds";
		public static final String RLID = "rlId";
		public static final String RESID = "resId";
		public static final String KEYWORD = "keyword";
		public static final String PACKAGE_NAME = "packageName";
		public static final String APP_NAME = "appName";
		public static final String APP_VER = "appVer";
		public static final String DURATION = "duration";
		public static final String DRUATION = "druation"; // sdk上报错误，mr计算也暂时按错误来解析
		public static final String LAUNCH_TIME = "launchTime";
		public static final String INSTALL_TIME = "installTime";
		public static final String UNINSTALL_TIME = "uninstallTime";
		public static final String MD5 = "md5";
		public static final String TITLE = "title";
		public static final String CONTENT = "content";
		public static final String PAGE_NAME = "pageName";
		public static final String LOGIN_TIME = "loginTime";
		public static final String RESUME_TIME = "resumeTime";
		public static final String ERROR_TIME = "errorTime";

	}

	public class Index {
		public static final int EVENTID = 0;
		public static final int STARTTIME = 1;
		public static final int ENDTIME = 2;
		public static final int DURATION = 3;
		public static final int EVENTATTR = 4;
	}

}
