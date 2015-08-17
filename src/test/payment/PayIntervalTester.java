package test.payment;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.UserInfoDayMapper;
import net.digitcube.hadoop.mapreduce.UserInfoDayReducer;
import net.digitcube.hadoop.mapreduce.accountmonitor.AccountNumPerDeviceMapper;
import net.digitcube.hadoop.mapreduce.accountmonitor.AccountNumPerDeviceReducer;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.mapreduce.event.EventStatisticsMapper;
import net.digitcube.hadoop.mapreduce.event.EventStatisticsReducer;
import net.digitcube.hadoop.mapreduce.level.UpgradeTimeStatMapper;
import net.digitcube.hadoop.mapreduce.level.UpgradeTimeStatReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourReducer;
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

public class PayIntervalTester {

	private MapReduceDriver<LongWritable, Text, Text, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> userInfoDayDriver;
	
	@Before
	public void setUp() {
		PayIntervalTestMapper map = new PayIntervalTestMapper();
		PayIntervalTestReducer red = new PayIntervalTestReducer();
		userInfoDayDriver = MapReduceDriver.newMapReduceDriver(map, red);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		
		LongWritable longWritable = new LongWritable();
		
		/*BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\online.0210")));
		String line = null;
		while(null != (line=br.readLine())){
			OnlineDayLog onlineDayLog = new OnlineDayLog(line.split("\t"));
			userInfoDayDriver.withInput(longWritable, new Text(onlineDayLog.getAccountID()
					+ "\tol\t" + onlineDayLog.getOnlineRecords()));
			//break;
		}
		br.close();
		
		br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\pay.0210")));
		while(null != (line=br.readLine())){
			PaymentDayLog paymentDayLog = new PaymentDayLog(line.split("\t"));
			userInfoDayDriver.withInput(longWritable, new Text(paymentDayLog.getAccountID()
					+ "\tpay\t" + paymentDayLog.getPayRecords()));
			//break;
		}
		br.close();
		
		
		br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\roll.0209")));
		while(null != (line=br.readLine())){
			UserInfoRollingLog userInfoRollingLog = new UserInfoRollingLog(Calendar.getInstance().getTime(), line.split("\t"));
			if("_ALL_GS".equals(userInfoRollingLog.getPlayerDayInfo().getGameRegion())){
				userInfoDayDriver.withInput(longWritable, new Text(userInfoRollingLog.getAccountID()
						+ "\troll\t" + userInfoRollingLog.getPlayerDayInfo().getTotalOnlineTime()));
			}
		}
		br.close();*/
		
		String online = "a15149078888	ol	1391961967:94,1391962717:1841,1392007910:517";
		String pay  = "a15149078888	pay	1391963069:2.0:0";
		String roll  = "a15149078888	roll	58375";
			
		userInfoDayDriver.withInput(longWritable, new Text(online));
		userInfoDayDriver.withInput(longWritable, new Text(pay));
		//userInfoDayDriver.withInput(longWritable, new Text(roll));
		userInfoDayDriver.run();
	}
}

