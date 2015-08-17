package net.digitcube.hadoop.common;

import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.util.StringUtil;

/**
 * 事件属性工具
 *  Title: AttrUtil.java<br>
 * Description: AttrUtil.java<br>
 * Copyright: Copyright (c) 2013 www.dataeye.com<br>
 * Company: DataEye 数据之眼<br>
 * 
 * @author Ivan <br>
 * @date 2013-11-9 <br>
 * @version 1.0 <br>
 */
public class AttrUtil {
	/**
	 * 从事件属性字符串中解析出属性map
	 * 
	 * @param eventAttrString
	 * @return
	 */
	public static Map<String, String> getEventAttrMap(String eventAttrString) {
		Map<String, String> map = new HashMap<String, String>();
		if (!StringUtil.isEmpty(eventAttrString)) {
			String[] attrArr = eventAttrString.split(",");
			if (attrArr.length > 0) {
				for (String attr : attrArr) {
					String[] keyValue = attr.split(":");
					if (keyValue.length == 2) {
						String key = keyValue[0];
						String value = keyValue[1];
						map.put(key, value);
					}
				}
			}
		}
		return map;
	}

	/**
	 * 取具体属性
	 * 
	 * @param eventAttrString
	 * @param key
	 * @return
	 */
	public static String getEventAttrValue(String eventAttrString, String key, String defaultValue) {
		Map<String, String> map = getEventAttrMap(eventAttrString);
		return map.get(key) == null ? defaultValue : map.get(key);
	}

	/**
	 * 取具体属性
	 * 
	 * @param eventAttrString
	 * @param key
	 * @return
	 */
	public static String getEventAttrValue(String eventAttrString, String key) {
		Map<String, String> map = getEventAttrMap(eventAttrString);
		return map.get(key);
	}
}
