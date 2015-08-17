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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.jce.UserExtendInfoLog.ExtendInfoDetail;
import net.digitcube.hadoop.jce.UserExtendInfoLog.ItemDayInfo;
import net.digitcube.hadoop.jce.UserExtendInfoLog.LevelDayInfo;
import net.digitcube.hadoop.jce.UserExtendInfoLog.TaskDayInfo;
import net.digitcube.hadoop.mapreduce.domain.UserInfoMonthRolling;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.mapreduce.domain.UserInfoWeekRolling;
import net.digitcube.hadoop.model.UserExtInfoRollingLog;

public class UserExtInfoRollingReader {

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
		
		String line = null;
		Calendar cal = Calendar.getInstance();
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\auser.tasksum"));
		while(null != (line = br.readLine())){
			
			String[] arr = line.split(MRConstants.SEPERATOR_IN);
			UserExtInfoRollingLog extInfoRollingLog = new UserExtInfoRollingLog(arr);
			Set<Entry<Integer, ExtendInfoDetail>> set = extInfoRollingLog.getDetailMap().getDetailMap().entrySet();
			for(Entry<Integer, ExtendInfoDetail> entry : set){
				cal.setTimeInMillis(1000L * entry.getKey());
				System.out.println("date = " + cal.getTime() + "------------------------");
				ArrayList<ItemDayInfo> list = entry.getValue().getItemDayInfoList();
				System.out.println("items = " + list.size() + "------------------------");
				for(ItemDayInfo info : list){
					System.out.println(
							info.getItemId() + "\t" + 
							info.getItemType() + "\t" +
							info.getBuyCount() + "\t" +
							info.getGainCount() + "\t" +
							info.getUseCount() + "\t"
							);
				}
				
				ArrayList<LevelDayInfo> levelList = entry.getValue().getLevelDayInfoList();
				System.out.println("levels = " + levelList.size() + "------------------------");
				for(LevelDayInfo info : levelList){
					System.out.println(
							info.getLevelId() + "\t" + 
							info.getSeqno() + "\t" +
							info.getBeginTimes() + "\t" +
							info.getSuccessTimes() + "\t" +
							info.getFailueTimes() + "\t"
							);
				}
				
				
				ArrayList<TaskDayInfo> taskList = entry.getValue().getTaskDayInfoList();
				System.out.println("tasks = " + levelList.size() + "------------------------");
				for(TaskDayInfo info : taskList){
					System.out.println(
							info.getTaskId() + "\t" + 
							info.getTaskType() + "\t" +
							info.getBeginTimes() + "\t" +
							info.getSuccessTimes() + "\t" +
							info.getFailueTimes() + "\t"
							);
				}
			}
		}
		
		//bw.close();
		br.close();
	}
}
