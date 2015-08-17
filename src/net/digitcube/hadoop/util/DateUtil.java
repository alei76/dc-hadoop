package net.digitcube.hadoop.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.UtilConstants;

public class DateUtil {

	public static void main(String[] args) throws IOException {
		System.out.println("Start...");
		
		 /*Properties properties = new Properties();
		 properties.setProperty("database.type", "mysql");
		 properties.setProperty("database.url","jdbc:mysql://localhost/mydb");
		 properties.setProperty("database.username", "root");
		 properties.setProperty("database.password", "root");
		 storeProp2HadoopXML(properties,"D:/TEST.XML");*/
		

		/*Calendar rightNow = Calendar.getInstance();
		String scheduleTime = "53,30,46 15 14 * *";
		System.out.println("isJobOnTime : "+isJobOnTime(rightNow, scheduleTime));*/
		
		//System.out.println(Integer.valueOf("9-4"));
		
//		timeTest();
		
		Calendar cal = Calendar.getInstance();
		String inputPathRegex = "/input/[yyyy]/[MM-1~MM-2]/app/[dd-7,dd-15]/[HH-19~HH-15,1*,22,23]/part*";
		//String inputPathRegex = "/input/[yyyy]/[MM]/app/[dd]/[HH]/";
		List<String> list = parseForPath(inputPathRegex, cal);
		for(String s : list){
			System.out.println(s);
		}
		
		//Map<Integer, String> map = new HashMap<Integer, String>();
		/*Map<Integer, String> map = resolveDateFromTimeReg("-30,20,-10,-5~-3,1~9,10,*,2*,1*");
		for(Integer i : map.keySet()){
			System.out.println(i + " : " + map.get(i));
		}*/
		
		/*List<String> list = getDateOfNatureMonth(-5);
		for(String s : list){
			System.out.println(s);
		}*/
		System.out.println("End...");
	}

	public static boolean storeProp2HadoopXML(Properties prop, String xmlFile)
			throws IOException {

		String xmlHeaderVersion = "<?xml version=\"1.0\"?>";
		String xmlHeaderType = "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>";

		String confHeader = "<configuration>";
		String confTailer = "</configuration>";

		String propHeader = "<property>";
		String propTailer = "</property>";

		String nameHeader = "<name>";
		String nameTailer = "</name>";

		String valueHeader = "<value>";
		String valueTailer = "</value>";

		BufferedWriter xmlWriter = null;
		try {
			xmlWriter = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(xmlFile), "UTF-8"));

			xmlWriter.write(xmlHeaderVersion);
			xmlWriter.newLine();

			xmlWriter.write(xmlHeaderType);
			xmlWriter.newLine();

			xmlWriter.write(confHeader);
			xmlWriter.newLine();

			Set<Entry<Object, Object>> entSet = prop.entrySet();
			for (Entry<Object, Object> entry : entSet) {
				xmlWriter.write(propHeader);
				xmlWriter.newLine();

				xmlWriter
						.write("\t" + nameHeader + entry.getKey() + nameTailer);
				xmlWriter.newLine();

				xmlWriter.write("\t" + valueHeader + entry.getValue()
						+ valueTailer);
				xmlWriter.newLine();

				xmlWriter.write(propTailer);
				xmlWriter.newLine();
			}

			xmlWriter.write(confTailer);
			xmlWriter.newLine();
			xmlWriter.close();

		} finally {
			if (null != xmlWriter) {
				xmlWriter.close();
			}
		}

		return true;
	}

	public static boolean isJobOnTime(Calendar rightNow, String scheduleTime) {

		String now = rightNow.get(Calendar.MINUTE) + " "
				+ rightNow.get(Calendar.HOUR_OF_DAY) + " "
				+ rightNow.get(Calendar.DAY_OF_MONTH) + " "
				+ (rightNow.get(Calendar.MONTH) + 1) + " "
				+ rightNow.get(Calendar.DAY_OF_WEEK);
		System.out.println("now : " + now);

		String[] arrTime = scheduleTime.split(" ");

		String asterisk = "*";
		String minute = arrTime[0];
		List<String> minutePatternList = getPatternList(minute);

		String hour = arrTime[1];
		List<String> hourPatternList = getPatternList(hour);

		String day = arrTime[2];
		List<String> dayPatternList = getPatternList(day);

		String month = arrTime[3];
		List<String> monthPatternList = getPatternList(month);

		String week = arrTime[4];
		List<String> weekPatternList = new ArrayList<String>();
		if (asterisk.equals(week)) {
			String weekPattern = "[0123456]";
			weekPatternList.add(weekPattern);
		} else if (week.contains(",")) {
			String[] patternArr = week.split(",");
			for (String pattern : patternArr) {
				if (pattern.startsWith("0") && pattern.length() > 1) {
					pattern = pattern.replaceFirst("0", "");
				}
				weekPatternList.add(pattern);
			}
		} else {
			weekPatternList.add(week);
		}

		List<String> patterns = new ArrayList<String>();
		for (String minutePat : minutePatternList) {
			for (String hourPat : hourPatternList) {
				for (String dayPat : dayPatternList) {
					for (String monthPat : monthPatternList) {
						for (String weekPat : weekPatternList) {
							patterns.add(minutePat + " " + hourPat + " " + dayPat + " " + monthPat + " " + weekPat);
						}
					}
				}
			}
		}

		boolean isOnTime = false;
		for (String p : patterns) {
			System.out.println("p : " + p + " : " + Pattern.compile(p).matcher(now).matches());
			if(Pattern.compile(p).matcher(now).matches()){
				isOnTime = true;
			}
		}
		
		return isOnTime;
	}

	private static List<String> getPatternList(String timeStr) {

		List<String> list = new ArrayList<String>();
		String asterisk = "*";
		if (asterisk.equals(timeStr)) {
			String pattern = "\\d{1,2}";
			list.add(pattern);
		} else if (timeStr.contains(",")) {
			String[] patternArr = timeStr.split(",");
			for (String pattern : patternArr) {
				if (pattern.startsWith("0") && pattern.length() > 1) {
					pattern = pattern.replaceFirst("0", "");
				}
				list.add(pattern);
			}
		} else if (timeStr.startsWith("0") && timeStr.length() > 1) {
			timeStr = timeStr.replaceFirst("0", "");
			list.add(timeStr);
		} else {
			list.add(timeStr);
		}

		return list;
	}

	public static void timeTest(){
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, -5);
		System.out.println(cal.getActualMaximum(Calendar.DATE));
	}
	
	public static List<Integer> getDateBeforeN(int days,int statitime){
		List<Integer> dateBeforeN = new ArrayList<Integer>();	
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(statitime*1000L);		
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		
		dateBeforeN.add(statitime);
		for(int i=1; i < days; i++){
			calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH) - 1);
			dateBeforeN.add(Integer.valueOf(calendar.getTimeInMillis()/1000 + ""));
		}
		
		return dateBeforeN;
	}	
	
	public static Integer getDateBeforeDays(int days,int statitime){		
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(statitime*1000L);		
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH) - days+1);	
		
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
		System.out.println(sdf.format(calendar.getTime()));
		
		return Integer.valueOf(calendar.getTimeInMillis()/1000 + "");
	}
	
	public static List<String> getDateOfNatureWeek(int weeksAgo){
		List<String> dateOfWeek = new ArrayList<String>();
		
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		cal.setFirstDayOfWeek(Calendar.SUNDAY);
		cal.add(Calendar.DATE, 1 - cal.get(Calendar.DAY_OF_WEEK));

		cal.add(Calendar.WEEK_OF_MONTH, weeksAgo);
		
		for(int i=1; i<8; i++){
			cal.set(Calendar.DAY_OF_WEEK, i);
			dateOfWeek.add(sdf.format(cal.getTime()));
		}
		
		return dateOfWeek;
	}
	
	public static List<String> getDateOfNatureMonth(int monthAgo){
		List<String> dateOfMonth = new ArrayList<String>();
		
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();

		cal.add(Calendar.MONTH, monthAgo);
		
		int maxDateOfMonth = cal.getActualMaximum(Calendar.DATE);
		for(int i = 1; i <= maxDateOfMonth; i++){
			cal.set(Calendar.DATE, i);
			dateOfMonth.add(sdf.format(cal.getTime()));
		}
		
		return dateOfMonth;
	}
	
	public static List<String> parseForPath(String inputPathRegex, Calendar cal){
		
		//返回结果
		List<String> inuptList = new ArrayList<String>();
		
		int yearBeginIndex = inputPathRegex.indexOf(UtilConstants.LEFT_BRACE);
		int yearEndIndex = inputPathRegex.indexOf(UtilConstants.RIGHT_BRACE, yearBeginIndex);
		String yearRegex = inputPathRegex.substring(yearBeginIndex, yearEndIndex + 1);
		
		int monthBeginIndex = inputPathRegex.indexOf(UtilConstants.LEFT_BRACE, yearEndIndex);
		int monthEndIndex = inputPathRegex.indexOf(UtilConstants.RIGHT_BRACE, monthBeginIndex);
		String monthRegex = inputPathRegex.substring(monthBeginIndex, monthEndIndex + 1);
		
		
		int dayBeginIndex = inputPathRegex.indexOf(UtilConstants.LEFT_BRACE, monthEndIndex);
		int dayEndIndex = inputPathRegex.indexOf(UtilConstants.RIGHT_BRACE, dayBeginIndex);
		String dayRegex = inputPathRegex.substring(dayBeginIndex, dayEndIndex + 1);
		
		int hourBeginIndex = inputPathRegex.indexOf(UtilConstants.LEFT_BRACE, dayEndIndex);
		int hourEndIndex = inputPathRegex.indexOf(UtilConstants.RIGHT_BRACE, hourBeginIndex);
		String hourRegex = inputPathRegex.substring(hourBeginIndex, hourEndIndex + 1);
		
		String yearStr = yearRegex.replace(UtilConstants.LEFT_BRACE, "")
								  .replace(UtilConstants.RIGHT_BRACE, "")
								  //.replace(UtilConstants.OPERATOR_MINUS, "")
								  .replace(UtilConstants.YEAR_FLAG, "")
								  .replaceAll(" ", "");
		
		String monthStr = monthRegex.replace(UtilConstants.LEFT_BRACE, "")
								  	.replace(UtilConstants.RIGHT_BRACE, "")
								  	//.replace(UtilConstants.OPERATOR_MINUS, "")
								  	.replace(UtilConstants.MONTH_FLAG, "")
								  	.replaceAll(" ", "");
		
		String dayStr = dayRegex.replace(UtilConstants.LEFT_BRACE, "")
								.replace(UtilConstants.RIGHT_BRACE, "")
								//.replace(UtilConstants.OPERATOR_MINUS, "")
								.replace(UtilConstants.DAY_FLAG, "")
								.replaceAll(" ", "");
		
		String hourStr = hourRegex.replace(UtilConstants.LEFT_BRACE, "")
								  .replace(UtilConstants.RIGHT_BRACE, "")
								  //.replace(UtilConstants.OPERATOR_MINUS, "")
								  .replace(UtilConstants.HOUR_FLAG, "")
								  .replaceAll(" ", "");
		
		Map<Integer, String> yearMap = resolveDateFromTimeReg(yearStr);
		Map<Integer, String> monthMap = resolveDateFromTimeReg(monthStr);
		Map<Integer, String> dayMap = resolveDateFromTimeReg(dayStr);
		Map<Integer, String> hourMap = resolveDateFromTimeReg(hourStr);
		
		Set<Integer> yearKeySet = yearMap.keySet();
		Set<Integer> monthKeySet = monthMap.keySet();
		Set<Integer> dayKeySet = dayMap.keySet();
		Set<Integer> hourKeySet = hourMap.keySet();
		
		int originalYear = cal.get(Calendar.YEAR);
		int originalMonth = cal.get(Calendar.MONTH);
		int originalDay = cal.get(Calendar.DATE);
		int originalHour = cal.get(Calendar.HOUR_OF_DAY);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		for(Integer yearOffSet : yearKeySet){

			for(Integer monthOffSet : monthKeySet){
				
				for(Integer dayOffSet : dayKeySet){
					
					for(Integer hourOffSet : hourKeySet){
						
						cal.set(Calendar.YEAR, originalYear);
						cal.set(Calendar.MONTH, originalMonth);
						cal.set(Calendar.DATE, originalDay);
						cal.set(Calendar.HOUR_OF_DAY, originalHour);
						
						String yearOperator = yearMap.get(yearOffSet);
						if(UtilConstants.ADD_DATE_FLAG.equals(yearOperator)){
							cal.add(Calendar.YEAR, yearOffSet);
						}else if(UtilConstants.SET_DATE_FLAG.equals(yearOperator)){
							cal.set(Calendar.YEAR, yearOffSet);
						}
						
						String monthOperator = monthMap.get(monthOffSet);
						if(UtilConstants.ADD_DATE_FLAG.equals(monthOperator)){
							cal.add(Calendar.MONTH, monthOffSet);
						}else if(UtilConstants.SET_DATE_FLAG.equals(monthOperator)){
							cal.set(Calendar.MONTH, monthOffSet);
						}

						String dayOperator = dayMap.get(dayOffSet);
						if(UtilConstants.ADD_DATE_FLAG.equals(dayOperator)){
							cal.add(Calendar.DATE, dayOffSet);
						}else if(UtilConstants.SET_DATE_FLAG.equals(dayOperator)){
							cal.set(Calendar.DATE, dayOffSet);
						}
						
						String hourOperator = hourMap.get(hourOffSet);
						if(UtilConstants.ADD_DATE_FLAG.equals(hourOperator)){
							cal.add(Calendar.HOUR_OF_DAY, hourOffSet);
						}else if(UtilConstants.SET_DATE_FLAG.equals(hourOperator)){
							cal.set(Calendar.HOUR_OF_DAY, hourOffSet);
						}
						
						String yyyyMMdd = sdf.format(cal.getTime());
						String year = yyyyMMdd.substring(0, 4);
						if(yearOperator.contains(UtilConstants.ALL_FLAG)){
							year = yearOperator;
						}
						
						String month = yyyyMMdd.substring(4, 6);
						if(monthOperator.contains(UtilConstants.ALL_FLAG)){
							month = monthOperator;
						}
						
						String day = yyyyMMdd.substring(6, 8);
						if(dayOperator.contains(UtilConstants.ALL_FLAG)){
							day = dayOperator;
						}
						
						String hour = yyyyMMdd.substring(8, 10);
						if(hourOperator.contains(UtilConstants.ALL_FLAG)){
							hour = hourOperator;
						}
						
						String resultPath = inputPathRegex.replace(yearRegex, year)
								  					  .replace(monthRegex, month)
								  					  .replace(dayRegex, day)
								  					  .replace(hourRegex, hour);
						//System.out.println(resultPath);
						inuptList.add(resultPath);
					}
				}
			}
		}
		
		Collections.sort(inuptList);
		return inuptList;
	}
	
	private static Map<Integer, String> resolveDateFromTimeReg(String timeRegex){
		
		Map<Integer, String> resultMap = new HashMap<Integer, String>();
		
		if(timeRegex.contains(UtilConstants.OPERATOR_COMMA)){
			String[] dateCommaArr = timeRegex.split(UtilConstants.OPERATOR_COMMA);
			for(String currentDate : dateCommaArr){
				if(currentDate.contains(UtilConstants.OPERATOR_RANGE)){
					resolveDatesByRange(currentDate, resultMap);
				}else{
					resolveDateBySingleDate(currentDate, resultMap);
				}
				/*else if(currentDate.contains(UtilConstants.ALL_FLAG)){
					resultMap.put(allIndicator--, currentDate);
				}else if("".equals(currentDate)){
					resultMap.put(0, UtilConstants.ADD_DATE_FLAG);
				}else{
					if(currentDate.contains(UtilConstants.OPERATOR_MINUS)){
						resultMap.put(Integer.valueOf(currentDate), UtilConstants.ADD_DATE_FLAG);
					}else{
						resultMap.put(Integer.valueOf(currentDate), UtilConstants.SET_DATE_FLAG);
					}
				}*/
			}
		}else if(timeRegex.contains(UtilConstants.OPERATOR_RANGE)){
			resolveDatesByRange(timeRegex, resultMap);
		}else{
			resolveDateBySingleDate(timeRegex, resultMap);
		}
		
		return resultMap;
	}
	
	private static void resolveDatesByRange(String rangeDate, Map<Integer, String> resultMap){
		if(rangeDate.contains(UtilConstants.OPERATOR_RANGE)){
			String setOrAddFlag = rangeDate.contains(UtilConstants.OPERATOR_MINUS) ? UtilConstants.ADD_DATE_FLAG : UtilConstants.SET_DATE_FLAG;
			String[] dateRangeArr = rangeDate.split(UtilConstants.OPERATOR_RANGE);
			int value1 = Integer.valueOf(dateRangeArr[0]);
			int value2 = Integer.valueOf(dateRangeArr[1]);
			int greatOne = Math.max(value1, value2);
			int lessOne = Math.min(value1, value2);
			for(int i = lessOne; i <= greatOne; i++){
				resultMap.put(i, setOrAddFlag);
			}
		}
	}
	
	private static void resolveDateBySingleDate(String currentDate, Map<Integer, String> resultMap){
		if(currentDate.contains(UtilConstants.ALL_FLAG)){
			resultMap.put(0 - resultMap.size(), currentDate);
		}else if("".equals(currentDate)){
			resultMap.put(0, UtilConstants.ADD_DATE_FLAG);
		}else{
			if(currentDate.contains(UtilConstants.OPERATOR_MINUS)){
				resultMap.put(Integer.valueOf(currentDate), UtilConstants.ADD_DATE_FLAG);
			}else{
				resultMap.put(Integer.valueOf(currentDate), UtilConstants.SET_DATE_FLAG);
			}
		}
	}
	
	/**
	 * 获取结算的数据的日期，时间为凌晨零点
	 * 如果今天调度的任务，计算昨天的数据，那么该方法返回的是昨天凌晨零点的时间
	 * @param conf
	 * @return
	 */
	public static int getStatDate(Configuration conf) {
		Date date = ConfigManager.getInitialDate(conf);
		Calendar calendar = Calendar.getInstance();
		if (date != null) {
			calendar.setTime(date);
		}
		calendar.add(Calendar.DAY_OF_MONTH, -1);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return (int) (calendar.getTimeInMillis() / 1000);
	}
	
	/**
	 * 获取周第一天的时间，记为周的结算时间
	 * 历史原因，这里认为一周的开始和结束分别为周一和周日
	 * 周任务规定在周一调度，所以调度时间 -7 即为上周的结算时间
	 * @param conf
	 * @return
	 */
	public static int getStatWeekDate(Configuration conf) {
		Date date = ConfigManager.getInitialDate(conf);
		Calendar calendar = Calendar.getInstance();
		if (date != null) {
			calendar.setTime(date);
		}
		calendar.add(Calendar.DAY_OF_MONTH, -7);// 结算时间默认调度时间的前7天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return (int) (calendar.getTimeInMillis() / 1000);
	}
	
	/**
	 * 获取月第一天的时间，记为月的结算时间
	 * 月任务规定在 1 号调度，所以调度时间 月-1  即为上月的结算时间
	 * @param conf
	 * @return
	 */
	public static int getStatMonthDate(Configuration conf) {
		Date date = ConfigManager.getInitialDate(conf);
		Calendar calendar = Calendar.getInstance();
		if (date != null) {
			calendar.setTime(date);
		}
		calendar.add(Calendar.MONTH, -1);// 结算时间默认调度时间的前1月1号
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return (int) (calendar.getTimeInMillis() / 1000);
	}
	
	/**
	 * 获取结算的数据的日期，时间为凌晨零点
	 * 如果是小时任务，则结算时间为当天凌晨零点
	 * @param conf
	 * @return
	 */
	public static int getStatDateForToday(Configuration conf) {
		Date date = ConfigManager.getInitialDate(conf);
		Calendar calendar = Calendar.getInstance();
		if (date != null) {
			calendar.setTime(date);
		}
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return (int) (calendar.getTimeInMillis() / 1000);
	}
	
	/**
	 * 获取小时或天任务的结算时间
	 * @param conf
	 * @return
	 */
	public static int getStatDateForHourOrToday(Configuration conf) {
		Date date = ConfigManager.getInitialDate(conf);
		Calendar calendar = Calendar.getInstance();
		if (date != null) {
			calendar.setTime(date);
		}
		
		// 如果不是小时任务则结算天减一
		if(!conf.getBoolean("dc.ishour.job", false)){
			calendar.add(Calendar.DAY_OF_MONTH, -1);
		}
		
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return (int) (calendar.getTimeInMillis() / 1000);
	}
	
	public static int getHourByMsecond(long msecond){
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(msecond);
		return calendar.get(Calendar.HOUR_OF_DAY);		
	}	

}



