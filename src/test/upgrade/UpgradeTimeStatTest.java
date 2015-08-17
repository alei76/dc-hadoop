package test.upgrade;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.accountmonitor.AccountNumPerDeviceMapper;
import net.digitcube.hadoop.mapreduce.accountmonitor.AccountNumPerDeviceReducer;
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

public class UpgradeTimeStatTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> upgradeDriver;
	
	@Before
	public void setUp() {
		UpgradeTimeStatMapper map2 = new UpgradeTimeStatMapper();
		UpgradeTimeStatReducer red2 = new UpgradeTimeStatReducer();
		upgradeDriver = MapReduceDriver.newMapReduceDriver(map2, red2);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		
		LongWritable longWritable = new LongWritable();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\upgrade.0108")));
		String line = null;
		while(null != (line=br.readLine())){
			upgradeDriver.withInput(longWritable, new Text(line));
		}
		List<Pair<OutFieldsBaseModel, IntWritable>> upgradeList = upgradeDriver.run();
		for(Pair<OutFieldsBaseModel, IntWritable> pair : upgradeList){
			String result = pair.getFirst().toString();
			if(result.contains("_ALL_GS")){
				System.out.println(pair.getFirst().toString() + "\t" + pair.getSecond().toString());
			}
		}
		br.close();
	}
}

