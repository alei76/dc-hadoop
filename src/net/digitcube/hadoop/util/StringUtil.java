package net.digitcube.hadoop.util;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapreduce.Mapper.Context;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.jce.GZIPUtils;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonParser;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月15日 下午5:17:06 @copyrigt www.digitcube.net
 */

public class StringUtil {
	public static Gson gson = new Gson();

	public static JsonParser jsonParser = new JsonParser();
	
	public static final Calendar calendar = Calendar.getInstance(); // 多线程使用不安全
	
	private static final int UNIXTIME_20130701 =  1372608000;

	public static int convertInt(String intStr, int defaultValue) {
		try {
			return Integer.valueOf(intStr).intValue();
		} catch (Throwable t1) {
			try {
				return Float.valueOf(intStr).intValue();
			} catch (Throwable t2) {
				return defaultValue;
			}
		}
	}

	public static long convertLong(String longStr, long defaultValue) {
		try {
			return Long.valueOf(longStr);
		} catch (Throwable t) {
			return defaultValue;
		}
	}

	public static float convertFloat(String floatStr, float defaultValue) {
		try {
			return Float.valueOf(floatStr);
		} catch (Throwable t) {
			return defaultValue;
		}
	}

	public static double convertDouble(String doubleStr, double defaultValue) {
		try {
			return Double.valueOf(doubleStr);
		} catch (Throwable t) {
			return defaultValue;
		}
	}

	public static BigDecimal convertBigDecimal(String bigDecimalDStr, int defaultValue) {
		try {
			return new BigDecimal(bigDecimalDStr);
		} catch (Throwable t) {
			return new BigDecimal(defaultValue);
		}
	}

	/**
	 * 分割字符串
	 * 
	 * @param line
	 *            原始字符串
	 * @param seperator
	 *            分隔符
	 * @return 分割结果
	 */
	public static String[] split(String line, String seperator) {
		if (line == null || seperator == null || seperator.length() == 0)
			return null;
		ArrayList<String> list = new ArrayList<String>();
		int pos1 = 0;
		int pos2;
		for (;;) {
			pos2 = line.indexOf(seperator, pos1);
			if (pos2 < 0) {
				list.add(line.substring(pos1));
				break;
			}
			list.add(line.substring(pos1, pos2));
			pos1 = pos2 + seperator.length();
		}
		// 去掉末尾的空串，和String.split行为保持一致
		for (int i = list.size() - 1; i >= 0 && list.get(i).length() == 0; --i) {
			list.remove(i);
		}
		return list.toArray(new String[0]);
	}

	private final static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
	private final static SimpleDateFormat yyyyMMddHH = new SimpleDateFormat("yyyyMMddHH");
	private final static SimpleDateFormat yyyyMMddHHmm = new SimpleDateFormat("yyyyMMddHHmm");
	private final static SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMddHHmmss");
	private final static SimpleDateFormat yyyy_MM_ddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static String date2yyyyMMdd(long timemilles) {
		return yyyyMMdd.format(new Date(timemilles));
	}

	public static int date2yyyyMMddInt(long timemilles) {
		return StringUtil.convertInt(date2yyyyMMdd(timemilles), 0);
	}

	public static String date_yyyyMMddHHmm(long timemilles) {
		return yyyyMMddHHmm.format(new Date(timemilles));
	}

	public static Date yyyyMMddHH2Date(String str) throws ParseException {
		return yyyyMMddHH.parse(str);
	}

	public static Date yyyy_MM_ddHHmmss2Date(String str) throws ParseException {
		return yyyy_MM_ddHHmmss.parse(str);
	}

	public static String date2yyyy_MM_ddHHmmss(Date date) {
		return yyyy_MM_ddHHmmss.format(date);
	}

	public static String date_yyyyMMddHHmmss(long timemilles) {
		return yyyyMMddHHmmss.format(new Date(timemilles));
	}

	public static boolean isEmpty(String str) {
		if (null == str || "".equals(str.trim())) {
			return true;
		}
		return false;
	}

	public static String getJsonStr(Object o) {
		String str = gson.toJson(o);
		return str;
	}

	public static Map<String, String> getMapFromJson(String json) {
		try {
			return gson.fromJson(json, new TypeToken<Map<String, String>>() {
			}.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	public static Set<String> getSetFromJson(String json) {
		try {
			return gson.fromJson(json, HashSet.class);
		} catch (Throwable t) {
			return null;
		}
	}

	/**
	 * Added by rickpan
	 * 
	 * @param json
	 * @param type
	 * @return
	 */
	public static <T1, T2> Map<T1, T2> getMapFromJson(String json, TypeToken<Map<T1, T2>> type) {
		try {
			return gson.fromJson(json, type.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	public static <T1, T2> Map<T1, T2> getMapFromJson(String json, T1 O, T2 P) {
		try {
			return gson.fromJson(json, new TypeToken<Map<T1, T2>>() {
			}.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	/**
	 * <pre>
	 * 空字符串转换为默认值
	 * @date 2015年4月23日 下午3:06:14
	 * @param src
	 * @param defaultValue
	 * @return
	 */
	public static String convertEmptyStr(String src, String defaultValue) {
		if (isEmpty(src)) {
			return defaultValue;
		}
		return src;
	}

	public static String getBase64Str(String json) {
		try {
			byte[] data = json.getBytes("UTF-8");
			data = GZIPUtils.compress(data); // 压缩
			return Base64Ext.encode(data);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return "";
	}

	public static String getStrFromBase64(String base64Str) {
		try {
			byte[] data = Base64Ext.decode(base64Str);
			data = GZIPUtils.decompress(data); // 解压
			return new String(data, "UTF-8");
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return "";
	}
	
	/**
	 * 由于历史原因，原始日志中 appId 和 version 合在同一字段 通过 | 先分割，格式为 appId|appVersion
	 * 
	 * @param appIdAndVer
	 * @return
	 */
	public static String[] getAppIdAndVer(String appIdAndVer) {
		String[] arr = appIdAndVer.split("\\|");
		if (arr.length < 1) {
			arr = new String[] { "-", "-" };
		} else if (arr.length < 2) {
			arr = new String[] { arr[0], "-" };
		} else {
			if (isEmpty(arr[0])) {
				arr[0] = "-";
			}
			if (isEmpty(arr[1])) {
				arr[1] = "-";
			}
		}
		return arr;
	}

	/**
	 * 将unixstamp时间戳转换为 去掉时分秒的整天时间戳
	 * 线程不安全
	 */
	public static int truncateDate(int unixTimestamp, int defaultValue) {
		calendar.setTimeInMillis(unixTimestamp * 1000L);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return (int) (calendar.getTimeInMillis() / 1000);
	}
	
	/**
	 * 将unixstamp时间戳转换为 去掉时分秒的整天时间戳
	 * 线程不安全
	 */
	public static int truncateDate(String unixTimestamp, int defaultValue) {
		return truncateDate(StringUtil.convertInt(unixTimestamp, 0), defaultValue);
	}
	
	/**
	 * <pre>
	 * 从appId和version混合字段中分割出appId
	 * @param appIdAndVer
	 * @return
	 */
	public static String getRealAppId(String appIdAndVer) {
		return getAppIdAndVer(appIdAndVer)[0];
	}

	/**
	 * <pre>
	 * 将字符串泛型类型的集合以特定的分隔符拼接
	 * @param cllection
	 * @param separator
	 * @return
	 */
	public static String join(Collection<String> collection, String separator) {
		StringBuilder sb = new StringBuilder();
		for (String item : collection) {
			sb.append(item).append(separator);
		}
		// 去掉最后的分隔符
		if (sb.length() == 0) {
			return "";
		} else {
			return sb.substring(0, sb.length() - separator.length());
		}
	}
	
	public static String join(String[] arr, String separator) {
		StringBuilder sb = new StringBuilder();
		for (String item : arr) {
			sb.append(item).append(separator);
		}
		// 去掉最后的分隔符
		if (sb.length() == 0) {
			return "";
		} else {
			return sb.substring(0, sb.length() - separator.length());
		}
	}

	public static String getFloatString(float value, int scale) {
		BigDecimal bd = new BigDecimal(value + "");
		bd = bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
		return bd.toString();
	}
	
	public static String[] merge(String[]... arrays) {
		int length = 0;
		for (String[] array : arrays) {
			length += array.length;
		}
		String[] mergeArray = new String[length];
		int i = 0;
		for (String[] array : arrays) {
			for (String item : array) {
				mergeArray[i++] = item;
			}
			length += array.length;
		}
		return mergeArray;
	}
	
	public static void main(String[] args) {
		List list = new ArrayList();
		Map map = new HashMap();
		map.put("name", "sam");
		map.put("age", 123);
		list.add(map);
		list.add(map);
		System.out.println(gson.toJson(map));
		System.out.println(gson.toJson(list));
		String appId = "|a";
		int i = 0;
		for (String s : getAppIdAndVer(appId)) {
			System.out.println(i++ + " : " + s);
		}

	}
}
