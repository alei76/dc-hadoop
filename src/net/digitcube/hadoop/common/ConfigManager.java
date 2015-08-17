package net.digitcube.hadoop.common;

import java.text.ParseException;
import java.util.Date;

import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.conf.Configuration;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月22日 下午3:53:20 @copyrigt www.digitcube.net
 */

public class ConfigManager {

	// 获取任务被实例化的时间
	public static Date getInitialDate(Configuration conf) {
		String logCreateTime = conf.get("job.schedule.time");
		if (logCreateTime != null && !"".equals(logCreateTime.trim())) {
			try {
				return StringUtil.yyyy_MM_ddHHmmss2Date(logCreateTime);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	/**
	 * 从配置文件中获取调度时间，如果不存在则返回默认值
	 * @param conf
	 * @param defaultDate
	 * @return
	 */
	public static Date getInitialDate(Configuration conf, Date defaultDate) {
		String logCreateTime = conf.get("job.schedule.time");
		if (logCreateTime != null && !"".equals(logCreateTime.trim())) {
			try {
				return StringUtil.yyyy_MM_ddHHmmss2Date(logCreateTime);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return defaultDate;
	}
}
