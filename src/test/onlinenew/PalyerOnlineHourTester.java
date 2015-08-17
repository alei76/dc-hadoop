package test.onlinenew;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class PalyerOnlineHourTester {

	//private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		OnlineHourMapper map = new OnlineHourMapper();
		OnlineHourReducer reduce = new OnlineHourReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testPalyerOnlineHour() throws Exception {
		List<String> dataList = TestDataUtil.generateOnlineData(60, 10, 1);
		LongWritable longWritable = new LongWritable();
		for(String data :  dataList){
			mapReduceDriver.withInput(longWritable, new Text(data));
		}
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = mapReduceDriver.run();
		
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			System.out.println(pair.getFirst().toString());
		}
	}
}

