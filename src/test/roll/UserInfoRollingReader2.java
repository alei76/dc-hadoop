package test.roll;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.jce.OnlineDay;
import net.digitcube.hadoop.mapreduce.domain.UserInfoMonthRolling;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.mapreduce.domain.UserInfoWeekRolling;

public class UserInfoRollingReader2 {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.out.println("start...");
		dayRollingReader();
//		weekRollingReader();
		//monthRollingReader();
		System.out.println("end...");
	}
	
	private static void dayRollingReader() throws Exception{
		
		//SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -7);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		int statDate = (int)(cal.getTimeInMillis()/1000);
		
		String line = null;
		Calendar cal2 = Calendar.getInstance();
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\qishao"));
		BufferedWriter bw = new BufferedWriter(new FileWriter("C:\\Users\\Administrator\\Desktop\\qishao.csv"));
		bw.write("新增日期,AccountId,新增日在线时长,首日付费,次日留存");
		bw.newLine();
		while(null != (line = br.readLine())){
			
			UserInfoRollingLog player = new UserInfoRollingLog(cal.getTime(), line.split("\t"));
			int firstLoginDate = player.getPlayerDayInfo().getFirstLoginDate(); 
			if(firstLoginDate < statDate || 
					!MRConstants.ALL_GAMESERVER.equals(player.getPlayerDayInfo().getGameRegion())){
				continue;
			}
			
			int nextDate =  firstLoginDate + 24 * 3600;
					
			cal2.setTimeInMillis(1000L * firstLoginDate);
			String newAddDate = sdf.format(cal2.getTime());
			String isPayAtFirstDay = firstLoginDate == player.getPlayerDayInfo().getFirstPayDate() ? "Y" : "N";
			String isStayNextDay = "N";
			int onlineTime = 0;
			java.util.ArrayList<OnlineDay> list = player.getPlayerDayInfo().getOnlineDayList();
			if(null == list){
				return;
			}
			for(OnlineDay online : list){
				if(firstLoginDate == online.getOnlineDate()){
					onlineTime = online.getOnlineTime();
					if(online.getPayTimes() > 0){
						isPayAtFirstDay = "Y";
					}
				}
				
				if(nextDate == online.getOnlineDate()){
					isStayNextDay = "Y";
				}
			}
			
			bw.write(newAddDate + ","
					+ player.getAccountID() + ","
					+ onlineTime + ","
					+ isPayAtFirstDay + ","
					+ isStayNextDay);
			bw.newLine();
		}
		bw.flush();
		
		Thread.sleep(3*1000);
		bw.close();
		br.close();
	}
}
