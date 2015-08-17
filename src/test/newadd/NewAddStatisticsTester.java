package test.newadd;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.newadd.ActDeviceNewPlayerMapper;
import net.digitcube.hadoop.mapreduce.newadd.ActDeviceNewPlayerReducer;
import net.digitcube.hadoop.mapreduce.newadd.NewAddStatisticsMapper;
import net.digitcube.hadoop.mapreduce.newadd.NewAddStatisticsReducer;
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

public class NewAddStatisticsTester {

	//private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> mapReduceDriver1;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, Text, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver2;
	
	@Before
	public void setUp() {
		ActDeviceNewPlayerMapper map = new ActDeviceNewPlayerMapper();
		ActDeviceNewPlayerReducer reduce = new ActDeviceNewPlayerReducer();
		mapReduceDriver1 = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		NewAddStatisticsMapper map2 =  new NewAddStatisticsMapper();
		NewAddStatisticsReducer reduce2 =  new NewAddStatisticsReducer();
		mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(map2, reduce2);
	}

	@Test
	public void testNewActDevicePlayer() throws Exception {
		List<String> actList = TestDataUtil.generateUserInfoData(10, 10);
		List<String> onlineList = TestDataUtil.generateOnlineData(30, 1, 15);
		List<String> payList = TestDataUtil.generatePayData(30, 10);
		List<String> actPlayerList = getActDevPalyer(actList, onlineList);
		System.out.println("actPlayerList size = "+actPlayerList.size());
		
		LongWritable longWritable = new LongWritable();
		//激活用户
		for(String data : actList){
			mapReduceDriver2.withInput(longWritable, new Text(data + "\t" + Constants.DATA_FLAG_DEVICE_ACT));
		}
		//新增用户
		for(String data : onlineList){
			mapReduceDriver2.withInput(longWritable, new Text(data + "\t" + Constants.DATA_FLAG_FIRST_ONLINE));
		}
		
		//激活设备中新增用户
		for(String data : actPlayerList){
			mapReduceDriver2.withInput(longWritable, new Text(data + "\t" + Constants.DATA_FLAG_AD_FONLINE));
		}
		
		//付费用户
		for(String data : payList){
			mapReduceDriver2.withInput(longWritable, new Text(data + "\t" + Constants.DATA_FLAG_FIRST_PAY));
		}
		
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> resultList = mapReduceDriver2.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : resultList){
			System.out.println(pair.getFirst().toString() + "\t" + pair.getSecond().toString());
		}
	}
	
	private List<String> getActDevPalyer(List<String> onlineList, List<String> infoList) throws IOException{
		
		LongWritable longWritable = new LongWritable();
		for(String data : onlineList){
			mapReduceDriver1.withInput(longWritable, new Text(data));
		}
		
		for(String data : infoList){
			mapReduceDriver1.withInput(longWritable, new Text(data));
		}
		List<Pair<OutFieldsBaseModel, NullWritable>> resultList = mapReduceDriver1.run();
		List<String> list = new ArrayList<String>();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : resultList){
			list.add(pair.getFirst().toString());
		}
		
		return list;
	}
}

