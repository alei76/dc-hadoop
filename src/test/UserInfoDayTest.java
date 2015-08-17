package test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.UserInfoDayMapper;
import net.digitcube.hadoop.mapreduce.UserInfoDayReducer;
import net.digitcube.hadoop.mapreduce.accountmonitor.AccountNumPerDeviceMapper;
import net.digitcube.hadoop.mapreduce.accountmonitor.AccountNumPerDeviceReducer;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.event.EventStatisticsMapper;
import net.digitcube.hadoop.mapreduce.event.EventStatisticsReducer;
import net.digitcube.hadoop.mapreduce.level.UpgradeTimeStatMapper;
import net.digitcube.hadoop.mapreduce.level.UpgradeTimeStatReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourReducer;
import net.digitcube.hadoop.model.UserInfoLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import test.onlinenew.TestDataUtil;

public class UserInfoDayTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> userInfoDayDriver;
	
	@Before
	public void setUp() {
		UserInfoDayMapper map2 = new UserInfoDayMapper();
		UserInfoDayReducer red2 = new UserInfoDayReducer();
		userInfoDayDriver = MapReduceDriver.newMapReduceDriver(map2, red2);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		Set<String> accIdSet = new HashSet<String>();
		LongWritable longWritable = new LongWritable();
		int lineNum = 0;
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\xingxing.1107")));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("C:\\Users\\Administrator\\Desktop\\1970_xx_1107.csv")));
		String line = null;
		while(null != (line=br.readLine())){
			lineNum++;
			String[] array = line.split(MRConstants.SEPERATOR_IN);
			UserInfoLog userInfoLog = new UserInfoLog(array);
			if("1970".equals(userInfoLog.getChannel())){
				accIdSet.add(userInfoLog.getChannel() + "," + userInfoLog.getExt5Map().get("IMEI"));
			}
			/*if(userInfoLog.isReferDomainEmpty() || StringUtil.isEmpty(userInfoLog.getH5ParentAccId())){
				return;
			}*/
			
			/*String accountId = userInfoLog.getAccountID();
			String[] appInfo = userInfoLog.getAppID().split("\\|");
			String[] keyFields = new String[] { appInfo[0],accountId };
			String[] valueFields = new String[] { "R",userInfoLog.getPlatform(),
					userInfoLog.getH5PromotionApp(),
					userInfoLog.getH5Domain(),
					userInfoLog.getH5Refer(),
					userInfoLog.getUID(),
					userInfoLog.getH5ParentAccId(),					
					//appInfo[1] 
			};*/
			
			/*String playerType = array[array.length - 1];
			String[] onlineDayArr = new String[array.length - 1];
			System.arraycopy(array, 0, onlineDayArr, 0, array.length - 1);
			OnlineDayLog onlineDayLog = new OnlineDayLog(onlineDayArr);
			if(playerType.contains("N") && MRConstants.ALL_GAMESERVER.equals(onlineDayLog.getExtend().getGameServer())){
				String[] onlineRecords = onlineDayLog.getOnlineRecords().split(",");
				if(onlineRecords.length < 1){
					continue;
				}
				
				String[] record = onlineRecords[0].split(":");
				String loginTime = record[0];
				int onlineTime = StringUtil.convertInt(record[1], 0);
				if(onlineTime >= 0 && onlineTime <= 4){
					//System.out.println("AccountID="+onlineDayLog.getAccountID() + ", onlineTime="+onlineTime);
					System.out.println(onlineTime + ","
							+ onlineDayLog.getAccountID() + ","
							+ loginTime + ","
							+ onlineDayLog.getExtend().getBrand() + ","
							+ onlineDayLog.getAppID().split("\\|")[1] + ","
							+ onlineDayLog.getMaxLevel()
							);
				}
			}*/
				
		}
		//List<Pair<OutFieldsBaseModel, NullWritable>> upgradeList = userInfoDayDriver.run();
		/*for(Pair<OutFieldsBaseModel, NullWritable> pair : upgradeList){
			String result = pair.getFirst().toString();
			if(result.contains("_ALL_GS")){
				System.out.println(pair.getFirst().toString() + "\t" + pair.getSecond().toString());
			}
		}*/
		br.close();
		/*int total = 0;
		for(Entry<Integer, Integer> entry : map.entrySet()){
			total += entry.getValue();
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}*/
		System.out.println("accIdSet.size="+accIdSet.size());
		for(String accId :  accIdSet){
			bw.write(accId);
			bw.newLine();
		}
		bw.close();
	}
}

