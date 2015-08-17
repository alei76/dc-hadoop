package test.newadd;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.newadd.ActDeviceNewPlayerMapper;
import net.digitcube.hadoop.mapreduce.newadd.ActDeviceNewPlayerReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import test.onlinenew.TestDataUtil;

public class NewActDevicePlayerTester {

	//private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		ActDeviceNewPlayerMapper map = new ActDeviceNewPlayerMapper();
		ActDeviceNewPlayerReducer reduce = new ActDeviceNewPlayerReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testNewActDevicePlayer() throws Exception {
		List<String> onlineList = TestDataUtil.generateOnlineData(60, 1, 15);
		List<String> infoList = TestDataUtil.generateUserInfoData(30, 30);
		
		LongWritable longWritable = new LongWritable();
		/*for(String data : onlineList){
			mapReduceDriver.withInput(longWritable, new Text(data));
		}
		for(String data : infoList){
			mapReduceDriver.withInput(longWritable, new Text(data));
		}*/
		
		String s1 = "1375859455	APPID1	00e662aa-8ba9-46d6-accf-8309ecb99ac4	00e662aa-8ba9-46d6-accf-8309ecb99ac4	IOS	channel5	3	1	13	gameServer2	800*480	opersystem	brand	2	中国	广东省	广州新一代数据中心	1375859402	95	1";
		String s2 = "1375875057	APPID1	00fea223-51d2-4b88-a98b-5b27376ff070	00fea223-51d2-4b88-a98b-5b27376ff070	ADR	channel5	3	1	25	gameServer4	800*480	opersystem	brand	2	中国	广东省	广州新一代数据中心	1375875003	28	1";
		String s3 = s1 + "\t" + "1234";
		String s4 = s2 + "\t" + "1234";
		mapReduceDriver.withInput(longWritable, new Text(s1));
		mapReduceDriver.withInput(longWritable, new Text(s2));
		mapReduceDriver.withInput(longWritable, new Text(s3));
		mapReduceDriver.withInput(longWritable, new Text(s4));
		
		List<Pair<OutFieldsBaseModel, NullWritable>> resultList = mapReduceDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : resultList){
			System.out.println(pair.getFirst().toString());
		}
		
	}
}

