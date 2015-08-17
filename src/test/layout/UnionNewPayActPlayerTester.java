package test.layout;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.layout.PayNewUnionActiveMapper;
import net.digitcube.hadoop.mapreduce.layout.PayNewUnionActiveReducer;
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

public class UnionNewPayActPlayerTester {

	//private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver;
	
	@Before
	public void setUp() {
		PayNewUnionActiveMapper map = new PayNewUnionActiveMapper();
		PayNewUnionActiveReducer reduce = new PayNewUnionActiveReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testNewActDevicePlayer() throws Exception {
		List<String> onlineList = TestDataUtil.generateOnlineData(60, 1, 15);
		List<String> infoList = TestDataUtil.generateUserInfoData(30, 30);
		
		LongWritable longWritable = new LongWritable();
		for(String data : onlineList){
			mapReduceDriver.withInput(longWritable, new Text(data));
		}
		for(String data : infoList){
			mapReduceDriver.withInput(longWritable, new Text(data));
		}
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> resultList = mapReduceDriver.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : resultList){
			System.out.println(pair.getFirst().toString());
		}
		
	}
}

