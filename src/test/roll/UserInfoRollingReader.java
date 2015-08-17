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
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.jce.OnlineDay;
import net.digitcube.hadoop.mapreduce.domain.UserInfoMonthRolling;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.mapreduce.domain.UserInfoWeekRolling;

public class UserInfoRollingReader {

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
		
		int total = 0;
		String line = null;
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\rolld.0322"));
		//BufferedWriter bw = new BufferedWriter(new FileWriter("C:\\Users\\Administrator\\Desktop\\dzresult.0610"));
		while(null != (line = br.readLine())){
			
			UserInfoRollingLog player = new UserInfoRollingLog(cal.getTime(), line.split("\t"));
			if(set.contains(player.getAccountID()) && !MRConstants.ALL_GAMESERVER.equals(player.getPlayerDayInfo().getGameRegion())){
				cal.setTimeInMillis(1000L*player.getPlayerDayInfo().getFirstLoginDate());
				String newAddDate = sdf.format(cal.getTime());
				System.out.println(player.getAccountID() + "\t" + newAddDate + "\t" + player.getPlayerDayInfo().getGameRegion());
			}
		}
		br.close();
	}

	private static Set<String> set  = new HashSet<String>();
	static{
		set.add("19613");
		set.add("19628");
		set.add("19631");
		set.add("19646");
		set.add("19664");
		set.add("19679");
		set.add("19682");
		set.add("19697");
		set.add("19709");
		set.add("19727");
		set.add("19730");
		set.add("19745");
		set.add("19763");
		set.add("19778");
		set.add("19781");
		set.add("19796");
		set.add("12105");
		set.add("19614");
		set.add("19629");
		set.add("19632");
		set.add("19647");
		set.add("19650");
		set.add("19665");
		set.add("19683");
		set.add("19698");
		set.add("19713");
		set.add("19728");
		set.add("19731");
		set.add("19746");
		set.add("19764");
		set.add("19779");
		set.add("19782");
		set.add("19797");
		set.add("19615");
		set.add("19633");
		set.add("19648");
		set.add("19651");
		set.add("19666");
		set.add("19684");
		set.add("19699");
		set.add("19714");
		set.add("19729");
		set.add("19732");
		set.add("19747");
		set.add("19750");
		set.add("19765");
		set.add("19783");
		set.add("19798");
		set.add("19571");
		set.add("19601");
		set.add("19634");
		set.add("19649");
		set.add("19652");
		set.add("19667");
		set.add("19670");
		set.add("19685");
		set.add("19700");
		set.add("19715");
		set.add("19733");
		set.add("19748");
		set.add("19751");
		set.add("19766");
		set.add("19784");
		set.add("19799");
		set.add("19587");
		set.add("19602");
		set.add("19620");
		set.add("19635");
		set.add("19653");
		set.add("19668");
		set.add("19671");
		set.add("19686");
		set.add("19701");
		set.add("19716");
		set.add("19734");
		set.add("19749");
		set.add("19752");
		set.add("19767");
		set.add("19785");
		set.add("19800");
		set.add("19603");
		set.add("19618");
		set.add("19621");
		set.add("19636");
		set.add("19654");
		set.add("19669");
		set.add("19672");
		set.add("19687");
		set.add("19690");
		set.add("19702");
		set.add("19717");
		set.add("19720");
		set.add("19735");
		set.add("19753");
		set.add("19768");
		set.add("19786");
		set.add("19801");
		set.add("17408");
		set.add("19604");
		set.add("19619");
		set.add("19637");
		set.add("19640");
		set.add("19655");
		set.add("19673");
		set.add("19688");
		set.add("19691");
		set.add("19703");
		set.add("19718");
		set.add("19721");
		set.add("19736");
		set.add("19754");
		set.add("19772");
		set.add("19787");
		set.add("19790");
		set.add("19802");
		set.add("19605");
		set.add("19623");
		set.add("19638");
		set.add("19641");
		set.add("19656");
		set.add("19674");
		set.add("19689");
		set.add("19692");
		set.add("19704");
		set.add("19719");
		set.add("19722");
		set.add("19737");
		set.add("19740");
		set.add("19755");
		set.add("19788");
		set.add("19791");
		set.add("19803");
		set.add("13747");
		set.add("19606");
		set.add("19624");
		set.add("19639");
		set.add("19642");
		set.add("19657");
		set.add("19660");
		set.add("19675");
		set.add("19693");
		set.add("19705");
		set.add("19723");
		set.add("19738");
		set.add("19741");
		set.add("19756");
		set.add("19774");
		set.add("19789");
		set.add("19792");
		set.add("19804");
		set.add("19607");
		set.add("19625");
		set.add("19643");
		set.add("19658");
		set.add("19661");
		set.add("19676");
		set.add("19694");
		set.add("19706");
		set.add("19724");
		set.add("19739");
		set.add("19742");
		set.add("19757");
		set.add("19760");
		set.add("19775");
		set.add("19793");
		set.add("19805");
		set.add("13080");
		set.add("19608");
		set.add("19611");
		set.add("19626");
		set.add("19644");
		set.add("19659");
		set.add("19662");
		set.add("19677");
		set.add("19680");
		set.add("19695");
		set.add("19707");
		set.add("19710");
		set.add("19725");
		set.add("19743");
		set.add("19758");
		set.add("19761");
		set.add("19776");
		set.add("19794");
		set.add("19806");
		set.add("19609");
		set.add("19645");
		set.add("19663");
		set.add("19678");
		set.add("19681");
		set.add("19696");
		set.add("19708");
		set.add("19711");
		set.add("19726");
		set.add("19744");
		set.add("19759");
		set.add("19777");
		set.add("19780");
		set.add("19795");
		set.add("19807");
	}
	private static void weekRollingReader() throws Exception{
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		
		int totalLine = 0;
		int totalLine2 = 0;
		String line = null;
		
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\USERROLLING_W.1104"));
		BufferedWriter bw =  new BufferedWriter(new FileWriter("C:\\Users\\Administrator\\Desktop\\USERROLLING_W.1104.result"));
		
		int statDate = (int)(cal.getTimeInMillis()/1000);
		
		while(null != (line = br.readLine())){
			totalLine++;
			UserInfoWeekRolling userInfoWeekRolling = new UserInfoWeekRolling(cal.getTime(), line.split("\t"));
			bw.write(userInfoWeekRolling.getAppID() + "\t"
					+userInfoWeekRolling.getAccountID() + "\t"
					+userInfoWeekRolling.getPlayerWeekInfo().getChannel() + "\t"
					+userInfoWeekRolling.getPlayerWeekInfo().getGameRegion());
			bw.newLine();
			
				
		}
		bw.close();
		br.close();
		System.out.println("totalLine = " + totalLine);
		System.out.println("totalLine2 = " + totalLine2);
		
	}

	private static void monthRollingReader() throws Exception{
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		
		int totalLine = 0;
		int totalLine2 = 0;
		String line = null;
		/*BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\ROLL\\USERROLLING_DAY.1107")));
		BufferedWriter bw =  new BufferedWriter(new OutputStreamWriter(new FileOutputStream("C:\\Users\\Administrator\\Desktop\\ROLL\\USERROLLING_DAY.1107.result")));
		BufferedWriter bw2 =  new BufferedWriter(new OutputStreamWriter(new FileOutputStream("C:\\Users\\Administrator\\Desktop\\ROLL\\USERROLLING_DAY.1107.result2")));*/
		
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\USERROLLING_M2.1109"));
		BufferedWriter bw =  new BufferedWriter(new FileWriter("C:\\Users\\Administrator\\Desktop\\USERROLLING_M2.1109.result"));
		
		int statDate = (int)(cal.getTimeInMillis()/1000);
		
		while(null != (line = br.readLine())){
			totalLine++;
			UserInfoMonthRolling userInfoWeekRolling = new UserInfoMonthRolling(cal.getTime(), line.split("\t"));
			bw.write(userInfoWeekRolling.getAppID() + "\t"
					+userInfoWeekRolling.getAccountID() + "\t"
					+userInfoWeekRolling.getPlayerMonthInfo().getChannel() + "\t"
					+userInfoWeekRolling.getPlayerMonthInfo().getGameRegion());
			bw.newLine();
			
				
		}
		bw.close();
		br.close();
		System.out.println("totalLine = " + totalLine);
		System.out.println("totalLine2 = " + totalLine2);
		
	}
}
