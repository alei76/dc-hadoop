package test.onlinenew;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.util.StringUtil;

public class OnlineDayLogReader {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		//onlineDayReader();
		String s = "	1628db7e26a436ba788358603d3fb3ad	1395370060617	2	com.warhegem.huawei	1	1	0	8F天下争雄	480x854	4.1.2	华为HUAWEI C8813Q	9	中国	甘肃省	中国联通	-	0	-	-	-	1396528100	171	19	";
		String[] arr = s.split(MRConstants.SEPERATOR_IN);
		OnlineLog onlineLog = new OnlineLog(arr);
	}

	private static void onlineDayReader() throws Exception{
		
		String line = null;
		
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\old.040321"));
		//BufferedWriter bw =  new BufferedWriter(new FileWriter("C:\\Users\\Administrator\\Desktop\\paymonth.1229.result"));
		while(null != (line = br.readLine())){
			String[] arr = line.split(MRConstants.SEPERATOR_IN);
			OnlineDayLog log = new OnlineDayLog(arr);
			String appId = log.getAppID();
			if(appId.split("\\|").length <2){
				System.out.println(line);
			}
		}
		//bw.close();
		br.close();
	}
}
