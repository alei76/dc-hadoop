package test.onlinenew;

import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
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

public class AcuPcuCntHourTest {

	//private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> dependDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> targetDriver;
	
	@Before
	public void setUp() {
		OnlineHourMapper map = new OnlineHourMapper();
		OnlineHourReducer reduce = new OnlineHourReducer();
		dependDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		AcuPcuCntMapper targetMap = new AcuPcuCntMapper();
		AcuPcuCntReducer targetReducer = new AcuPcuCntReducer();
		targetDriver = MapReduceDriver.newMapReduceDriver(targetMap, targetReducer);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		List<String> dataList = TestDataUtil.generateOnlineData(60, 10, 1);
		LongWritable longWritable = new LongWritable();
		for(String data :  dataList){
			dependDriver.withInput(longWritable, new Text(data));
		}
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = dependDriver.run();
		
		
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			System.out.println("---" + pair.getFirst().toString());
			targetDriver.withInput(longWritable, new Text(pair.getFirst().toString()));
		}
		System.out.println("\n\n\n");
		List<Pair<OutFieldsBaseModel, IntWritable>> acuPcuList = targetDriver.run();
		for(Pair<OutFieldsBaseModel, IntWritable> pair : acuPcuList){
			System.out.println("===" + pair.getFirst().toString() + " : " + pair.getSecond().get());
		}
	}
}

