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
import net.digitcube.hadoop.mapreduce.payment.Player30DayValueAndArpuMapper;
import net.digitcube.hadoop.mapreduce.payment.Player30DayValueAndArpuReducer;
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

public class Player30ARPDAUTester {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> userInfoDayDriver;
	
	@Before
	public void setUp() {
		Player30DayValueAndArpuMapper map = new Player30DayValueAndArpuMapper();
		Player30DayValueAndArpuReducer red = new Player30DayValueAndArpuReducer();
		userInfoDayDriver = MapReduceDriver.newMapReduceDriver(map, red);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		
		LongWritable longWritable = new LongWritable();
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\dizhu.0306")));
		String line = null;
		while(null != (line=br.readLine())){
			userInfoDayDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>>  list = userInfoDayDriver.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : list){
			System.out.println(pair.getFirst() + "\t" + pair.getSecond());
		}
	}
}

