package net.digitcube.hadoop.jce;

import java.text.SimpleDateFormat;
import java.util.Date;

import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5PlayerDayInfo;
import net.digitcube.hadoop.util.Base64Ext;
import net.digitcube.protocol.JceInputStream;

/**
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年7月27日 下午3:49:31 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class PlayInfoUtil {

	/**
	 * Hadoop 集群中有用到第三方 jar 包  dc-protocol.jar
	 * 该 jar 包主要用 JCE 协议对日、周、月滚存的用户信息进行存取
	 * 由于历史原因，2013.11.08 之前，该 jar 包内置默认的编码是 GBK
	 * 而客户端 SDK 以及 LogServer 都已升级为 UTF-8 编码，为一致性考虑
	 * 这里把 Hadoop 集群的  dc-protocol.jar 内置默认编码改为 UTF-8
	 * 
	 * JCE 编码修改后，为了对历史数据的兼容
	 * 这里需对日、周、月滚存设定一个时间点，与这些任务的调度时间比较做兼容处理：
	 * a)如果调度时间 < 设定时间点，那么 JCE 输入输出都用 GBK 编码
	 * b)如果调度时间 = 设定时间点，那么 JCE 输入用 GBK 编码，输出用 UTF-8 编码
	 * c)如果调度时间 > 设定时间点，那么 JCE 输入输出都用 UTF-8 编码
	 * 
	 */
	private  static final String dayRollTimePoint = "20131111";
	private  static final String weekRollTimePoint = "20131111"; //周一
	private  static final String monthRollTimePoint = "20131201"; //2013 年 12 月 1 号
	 
	public static PlayerDayInfo playDayInfoFromStr(String infoBase64, Date scheduleTime) {
		try {
			byte[] data = Base64Ext.decode(infoBase64);
			// 解压
			data = GZIPUtils.decompress(data);
			JceInputStream inputStream = new JceInputStream(data);
			
			//调度时间小于等于设定时间点 dayRollTimePoint，用 GBK 编码，否则用 UTF-8 编码
			String inputEncoding = getInputEncoding(scheduleTime, dayRollTimePoint);
			inputStream.setServerEncoding(inputEncoding);
			
			PlayerDayInfo playerDayInfo = new PlayerDayInfo();
			playerDayInfo.readFrom(inputStream);
			return playerDayInfo;
		} catch (Throwable t) {
			//t.printStackTrace();
		}
		return null;
	}
	
	public static H5PlayerDayInfo h5PlayDayInfoFromStr(String infoBase64, Date scheduleTime) {
		try {
			byte[] data = Base64Ext.decode(infoBase64);
			// 解压
			data = GZIPUtils.decompress(data);
			JceInputStream inputStream = new JceInputStream(data);
			
			//调度时间小于等于设定时间点 dayRollTimePoint，用 GBK 编码，否则用 UTF-8 编码
			String inputEncoding = getInputEncoding(scheduleTime, dayRollTimePoint);
			inputStream.setServerEncoding(inputEncoding);
			
			H5PlayerDayInfo playerDayInfo = new H5PlayerDayInfo();
			playerDayInfo.readFrom(inputStream);
			return playerDayInfo;
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}	
	

	public static String playDayInfo2Base64(PlayerDayInfo playerDayInfo, Date scheduleTime) {
		// 压缩
		try {
			//调度时间大于等于设定时间点 dayRollTimePoint，用 UTF-8 编码，否则用 GBK 编码
			String outputEncoding = getOutputEncoding(scheduleTime, dayRollTimePoint);
			//byte[] data = playerDayInfo.toByteArray();
			byte[] data = playerDayInfo.toByteArray(outputEncoding);
			
			data = GZIPUtils.compress(data);
			return Base64Ext.encode(data);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}
	
	public static String h5PlayDayInfo2Base64(H5PlayerDayInfo playerDayInfo, Date scheduleTime) {
		// 压缩
		try {			
			//调度时间大于等于设定时间点 dayRollTimePoint，用 UTF-8 编码，否则用 GBK 编码
			String outputEncoding = getOutputEncoding(scheduleTime, dayRollTimePoint);
			//byte[] data = playerDayInfo.toByteArray();
			byte[] data = playerDayInfo.toByteArray(outputEncoding);
			data = GZIPUtils.compress(data);
			return Base64Ext.encode(data);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}	

	public static PlayerWeekInfo playWeekInfoFromStr(String infoBase64, Date scheduleTime) {
		try {
			byte[] data = Base64Ext.decode(infoBase64);
			// 解压
			data = GZIPUtils.decompress(data);
			JceInputStream inputStream = new JceInputStream(data);
			
			//调度时间小于等于设定时间点 weekRollTimePoint，用 GBK 编码，否则用 UTF-8 编码
			String inputEncoding = getInputEncoding(scheduleTime, weekRollTimePoint);
			inputStream.setServerEncoding(inputEncoding);
			
			PlayerWeekInfo playerWeekInfo = new PlayerWeekInfo();
			playerWeekInfo.readFrom(inputStream);
			return playerWeekInfo;
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}

	public static String playWeek2Base64(PlayerWeekInfo playerWeekInfo, Date scheduleTime) {

		try {
			//调度时间大于等于设定时间点 weekRollTimePoint，用 UTF-8 编码，否则用 GBK 编码
			String outputEncoding = getOutputEncoding(scheduleTime, weekRollTimePoint);
			//byte[] data = playerDayInfo.toByteArray();
			byte[] data = playerWeekInfo.toByteArray(outputEncoding);
			
			// 压缩
			data = GZIPUtils.compress(data);
			return Base64Ext.encode(data);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}

	public static PlayerMonthInfo playMonthInfoFromStr(String infoBase64, Date scheduleTime) {
		try {
			byte[] data = Base64Ext.decode(infoBase64);
			// 解压
			data = GZIPUtils.decompress(data);
			JceInputStream inputStream = new JceInputStream(data);
			
			//调度时间小于等于设定时间点 weekRollTimePoint，用 GBK 编码，否则用 UTF-8 编码
			String inputEncoding = getInputEncoding(scheduleTime, monthRollTimePoint);
			inputStream.setServerEncoding(inputEncoding);
			
			PlayerMonthInfo playerMonthInfo = new PlayerMonthInfo();
			playerMonthInfo.readFrom(inputStream);
			return playerMonthInfo;
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}

	public static String playMonth2Base64(PlayerMonthInfo playerMontInfo, Date scheduleTime) {

		try {
			//调度时间大于等于设定时间点 weekRollTimePoint，用 UTF-8 编码，否则用 GBK 编码
			String outputEncoding = getOutputEncoding(scheduleTime, monthRollTimePoint);
			//byte[] data = playerDayInfo.toByteArray();
			byte[] data = playerMontInfo.toByteArray(outputEncoding);
			
			// 压缩
			data = GZIPUtils.compress(data);
			return Base64Ext.encode(data);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 调度时间小于等于设定时间点 timePoint，返回 GBK 编码，否则返回 UTF-8 编码
	 * @param scheduleTime
	 * @param timePoint
	 * @return
	 */
	private static String getInputEncoding(Date scheduleTime, String timePoint){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String yyyyMMdd = sdf.format(scheduleTime); 
		return yyyyMMdd.compareTo(timePoint) <= 0 ? "GBK" : "UTF-8";
	}
	/**
	 * 调度时间大于等于设定时间点 timePoint，返回  UTF-8 编码，否则返回  GBK 编码
	 * @param scheduleTime
	 * @param timePoint
	 * @return
	 */
	private static String getOutputEncoding(Date scheduleTime, String timePoint){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String yyyyMMdd = sdf.format(scheduleTime); 
		return yyyyMMdd.compareTo(timePoint) >= 0 ? "UTF-8" : "GBK";
	}
	
	/*public static PlayerDayInfo playDayInfoFromStr(String infoBase64) {
		try {
			byte[] data = Base64Ext.decode(infoBase64);
			// 解压
			data = GZIPUtils.decompress(data);
			JceInputStream inputStream = new JceInputStream(data);
			PlayerDayInfo playerDayInfo = new PlayerDayInfo();
			playerDayInfo.readFrom(inputStream);
			return playerDayInfo;
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}

	public static String playDayInfo2Base64(PlayerDayInfo playerDayInfo) {
		// 压缩
		try {
			byte[] data = playerDayInfo.toByteArray();
			data = GZIPUtils.compress(data);
			return Base64Ext.encode(data);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}

	public static PlayerWeekInfo playWeekInfoFromStr(String infoBase64) {
		try {
			byte[] data = Base64Ext.decode(infoBase64);
			// 解压
			data = GZIPUtils.decompress(data);
			JceInputStream inputStream = new JceInputStream(data);
			PlayerWeekInfo playerWeekInfo = new PlayerWeekInfo();
			playerWeekInfo.readFrom(inputStream);
			return playerWeekInfo;
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}

	public static String playWeek2Base64(PlayerWeekInfo playerWeekInfo) {

		try {
			byte[] data = playerWeekInfo.toByteArray();
			// 压缩
			data = GZIPUtils.compress(data);
			return Base64Ext.encode(data);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}

	public static PlayerMonthInfo playMonthInfoFromStr(String infoBase64) {
		try {
			byte[] data = Base64Ext.decode(infoBase64);
			// 解压
			data = GZIPUtils.decompress(data);
			JceInputStream inputStream = new JceInputStream(data);
			PlayerMonthInfo playerMonthInfo = new PlayerMonthInfo();
			playerMonthInfo.readFrom(inputStream);
			return playerMonthInfo;
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}

	public static String playMonth2Base64(PlayerMonthInfo playerMontInfo) {

		try {
			byte[] data = playerMontInfo.toByteArray();
			// 压缩
			data = GZIPUtils.compress(data);
			return Base64Ext.encode(data);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}*/
}
