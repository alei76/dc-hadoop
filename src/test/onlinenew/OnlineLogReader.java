package test.onlinenew;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.util.StringUtil;

public class OnlineLogReader {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		onlineDayReader();
	}

	private static void onlineDayReader() throws Exception{
		
		Set<String> accSet = new HashSet<String>();
		
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		
		int beginTime = (int)(cal.getTimeInMillis()/1000);
		int endTime = beginTime + 3600 * 24;
		
		String line = null;
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\xiaot"));
		//BufferedWriter bw =  new BufferedWriter(new FileWriter("C:\\Users\\Administrator\\Desktop\\paymonth.1229.result"));
		while(null != (line = br.readLine())){
			
			String[] arr = line.split(MRConstants.SEPERATOR_IN);
			OnlineLog log = new OnlineLog(arr);
			try{
				int createTime = Integer.valueOf(log.getH5CRtTime());
				if(beginTime<=createTime && createTime <= endTime){
					accSet.add(log.getAccountID());
					//System.out.println(log.getAccountID());
				}
			}catch(Throwable t){
				
			}
			
		}
		//bw.close();
		br.close();
		for(String acc : accSet){
			System.out.println(acc);
		}
		
	}
}
