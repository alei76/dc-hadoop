package test.accountmonitor;

import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.accountmonitor.AccountNumPerDeviceMapper;
import net.digitcube.hadoop.mapreduce.accountmonitor.AccountNumPerDeviceReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourReducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import test.onlinenew.TestDataUtil;

public class AccountNumPerDeviceTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> accountDriver;
	
	@Before
	public void setUp() {
		AccountNumPerDeviceMapper map = new AccountNumPerDeviceMapper();
		AccountNumPerDeviceReducer reduce = new AccountNumPerDeviceReducer();
		accountDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		List<String> dataList = TestDataUtil.generateUserInfoData(9, 6);
		
		LongWritable longWritable = new LongWritable();
		for(String data :  dataList){
			accountDriver.withInput(longWritable, new Text(data));
		}
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = accountDriver.run();
		
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			System.out.println("---" + pair.getFirst().toString());
		}
	}
}

